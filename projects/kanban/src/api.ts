export interface Card {
  id: string;
  title: string;
  column: string;
  position: number;
  done: boolean;
}

export interface Board {
  title: string;
  columns: string[];
  cards: Card[];
}

export interface Comment {
  id: string;
  author: string;
  ts: string;
  text: string;
}

export interface CardDetail extends Card {
  description: string;
  comments: Comment[];
}

export async function getBoard(): Promise<Board> {
  return fetch("/api/board").then((r) => r.json());
}

export async function getCard(id: string): Promise<Card | null> {
  const r = await fetch(`/api/cards/${id}`);
  if (r.status === 404) return null;
  return r.json();
}

export async function getCardDetail(id: string): Promise<CardDetail | null> {
  const r = await fetch(`/api/cards/${id}/detail`);
  if (r.status === 404) return null;
  return r.json();
}

export async function addComment(
  id: string,
  text: string,
  author?: string,
): Promise<Comment> {
  return fetch(`/api/cards/${id}/comments`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(author === undefined ? { text } : { author, text }),
  }).then((r) => r.json());
}

export async function addCard(column: string, title: string): Promise<Card> {
  return fetch("/api/cards", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ column, title }),
  }).then((r) => r.json());
}

export async function patchCard(
  id: string,
  patch: Partial<Card> & { description?: string },
): Promise<Card> {
  return fetch(`/api/cards/${id}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(patch),
  }).then((r) => r.json());
}
