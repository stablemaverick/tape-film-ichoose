/** Must load before any import of app routes that pull in `supabase.server`. */
if (!process.env.SUPABASE_URL) {
  process.env.SUPABASE_URL = "http://127.0.0.1:54321";
}
if (!process.env.SUPABASE_SERVICE_KEY) {
  process.env.SUPABASE_SERVICE_KEY = "test-service-role-key-for-e2e";
}
