/**
 * Cognito authentication helper using the Hosted UI OAuth2 PKCE flow.
 *
 * Config is injected at build time via Vite env vars:
 *   VITE_COGNITO_USER_POOL_ID
 *   VITE_COGNITO_APP_CLIENT_ID
 *   VITE_COGNITO_DOMAIN   (e.g. https://fraud-detection-dashboard.auth.us-east-1.amazoncognito.com)
 */

const POOL_ID = import.meta.env.VITE_COGNITO_USER_POOL_ID ?? "";
const CLIENT_ID = import.meta.env.VITE_COGNITO_APP_CLIENT_ID ?? "";
const COGNITO_DOMAIN = import.meta.env.VITE_COGNITO_DOMAIN ?? "";

const REDIRECT_URI = window.location.origin;
const TOKEN_KEY = "fraud_dashboard_id_token";
const REFRESH_KEY = "fraud_dashboard_refresh_token";

export function isAuthConfigured(): boolean {
  return !!(POOL_ID && CLIENT_ID && COGNITO_DOMAIN);
}

/** Build the Cognito Hosted UI login URL. */
export function loginUrl(): string {
  const params = new URLSearchParams({
    client_id: CLIENT_ID,
    response_type: "code",
    scope: "openid email profile",
    redirect_uri: REDIRECT_URI,
  });
  return `${COGNITO_DOMAIN}/login?${params}`;
}

/** Build the Cognito logout URL. */
export function logoutUrl(): string {
  const params = new URLSearchParams({
    client_id: CLIENT_ID,
    logout_uri: REDIRECT_URI,
  });
  return `${COGNITO_DOMAIN}/logout?${params}`;
}

/** Exchange an authorization code for tokens. */
export async function exchangeCode(code: string): Promise<boolean> {
  const body = new URLSearchParams({
    grant_type: "authorization_code",
    client_id: CLIENT_ID,
    code,
    redirect_uri: REDIRECT_URI,
  });

  try {
    const resp = await fetch(`${COGNITO_DOMAIN}/oauth2/token`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body,
    });
    if (!resp.ok) return false;
    const data = await resp.json();
    sessionStorage.setItem(TOKEN_KEY, data.id_token);
    if (data.refresh_token) {
      sessionStorage.setItem(REFRESH_KEY, data.refresh_token);
    }
    return true;
  } catch {
    return false;
  }
}

/** Get the stored ID token (null if not logged in). */
export function getIdToken(): string | null {
  return sessionStorage.getItem(TOKEN_KEY);
}

/** Check if the user is currently authenticated. */
export function isAuthenticated(): boolean {
  const token = getIdToken();
  if (!token) return false;
  // Basic JWT expiry check
  try {
    const payload = JSON.parse(atob(token.split(".")[1]));
    return payload.exp * 1000 > Date.now();
  } catch {
    return false;
  }
}

/** Parse email from the ID token. */
export function getUserEmail(): string | null {
  const token = getIdToken();
  if (!token) return null;
  try {
    const payload = JSON.parse(atob(token.split(".")[1]));
    return payload.email ?? null;
  } catch {
    return null;
  }
}

/** Clear stored tokens. */
export function clearTokens(): void {
  sessionStorage.removeItem(TOKEN_KEY);
  sessionStorage.removeItem(REFRESH_KEY);
}
