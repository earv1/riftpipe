import { defineConfig } from "vite";
import solid from "vite-plugin-solid";

export default defineConfig({
  // GitHub Pages serves a project site under /<repo>/. The deploy workflow sets
  // PAGES_BASE=/riftpipe/; local builds, dev, and the e2e harness stay at "/".
  base: (typeof process !== "undefined" && process.env.PAGES_BASE) || "/",
  plugins: [solid()],
  server: {
    port: 5173,
  },
  build: {
    outDir: "dist",
  },
});
