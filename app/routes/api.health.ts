export async function loader() {
  return Response.json({ status: "ok", app: "tape-film-ichoose" });
}