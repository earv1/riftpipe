-- riftpipe neovim bridge (session-local; no install).
--
-- Load per-invocation:
--   RIFTPIPE_BIN=/path/to/riftpipe \
--   RIFTPIPE_ARGS="share /path/file --pipe" \
--   nvim -c 'luafile /path/to/nvim/riftpipe.lua' /path/file
--
-- It spawns `riftpipe ... --pipe` and bridges the CURRENT buffer to its stdio:
--   * local buffer change  -> {"op":"snapshot","text":...} on the job's stdin
--     (riftpipe diffs it; only the delta crosses the network)
--   * remote edit op on stdout -> applied surgically via nvim_buf_set_text
--     (cursor/undo preserved), with echo-suppression so it doesn't loop back.
--
-- The file on disk is only read at startup; sync is buffer<->pipe (no file race).

local function parse_args()
  local bin = vim.env.RIFTPIPE_BIN or "riftpipe"
  local args = {}
  for a in string.gmatch(vim.env.RIFTPIPE_ARGS or "", "%S+") do
    table.insert(args, a)
  end
  return bin, args
end

-- Char offset into the whole doc -> (0-indexed row, 0-indexed BYTE col).
local function char_to_rowcol(buf, charpos)
  local lines = vim.api.nvim_buf_get_lines(buf, 0, -1, false)
  local remaining = charpos
  for i, line in ipairs(lines) do
    local lc = vim.fn.strchars(line)
    if remaining <= lc then
      return i - 1, vim.str_byteindex(line, remaining)
    end
    remaining = remaining - (lc + 1) -- account for the newline
  end
  local last = math.max(#lines - 1, 0)
  return last, #(lines[#lines] or "")
end

local function apply_remote(buf, op)
  if op.op == "insert" then
    local r, c = char_to_rowcol(buf, op.pos)
    vim.api.nvim_buf_set_text(buf, r, c, r, c, vim.split(op.text, "\n", { plain = true }))
  elseif op.op == "delete" then
    local r1, c1 = char_to_rowcol(buf, op.pos)
    local r2, c2 = char_to_rowcol(buf, op.pos + op.len)
    vim.api.nvim_buf_set_text(buf, r1, c1, r2, c2, { "" })
  elseif op.op == "snapshot" then
    vim.api.nvim_buf_set_lines(buf, 0, -1, false, vim.split(op.text, "\n", { plain = true }))
  end
end

local function start()
  local buf = vim.api.nvim_get_current_buf()
  local bin, args = parse_args()
  local cmd = { bin }
  for _, a in ipairs(args) do
    table.insert(cmd, a)
  end

  if vim.fn.executable(bin) == 0 then
    vim.notify("riftpipe: `" .. bin .. "` not found (set RIFTPIPE_BIN)", vim.log.levels.ERROR)
    return
  end

  local applying = false -- true while we apply a remote edit (suppress echo)
  local acc = "" -- stdout line accumulator

  local job = vim.fn.jobstart(cmd, {
    on_stdout = function(_, data)
      if not data then
        return
      end
      acc = acc .. table.concat(data, "\n") -- reconstruct raw chunk
      while true do
        local nl = acc:find("\n", 1, true)
        if not nl then
          break
        end
        local line = acc:sub(1, nl - 1)
        acc = acc:sub(nl + 1)
        if #line > 0 then
          local ok, op = pcall(vim.json.decode, line)
          if ok and type(op) == "table" and op.op then
            applying = true
            pcall(apply_remote, buf, op)
            applying = false
          end
        end
      end
    end,
    on_stderr = function() end, -- swallow riftpipe's human/ticket messages
    on_exit = function()
      vim.schedule(function()
        vim.notify("riftpipe: sync process exited", vim.log.levels.WARN)
      end)
    end,
  })

  if job <= 0 then
    vim.notify("riftpipe: failed to start `" .. bin .. "`", vim.log.levels.ERROR)
    return
  end

  local function send_snapshot()
    local text = table.concat(vim.api.nvim_buf_get_lines(buf, 0, -1, false), "\n")
    vim.fn.chansend(job, vim.json.encode({ op = "snapshot", text = text }) .. "\n")
  end
  send_snapshot() -- initial state

  -- Push the buffer on local change (coalesced to once per event-loop tick).
  -- While `applying` a remote edit, skip — that's the echo-suppression.
  local pending = false
  vim.api.nvim_buf_attach(buf, false, {
    on_lines = function()
      if applying or pending then
        return
      end
      pending = true
      vim.schedule(function()
        pending = false
        send_snapshot()
      end)
    end,
  })

  vim.notify("riftpipe: bridge attached", vim.log.levels.INFO)
end

start()
