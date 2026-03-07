import { useEffect, useState } from "react";
import {
  isAuthConfigured,
  isAuthenticated,
  exchangeCode,
  loginUrl,
  logoutUrl,
  getUserEmail,
  clearTokens,
} from "../auth";

interface Props {
  children: React.ReactNode;
}

/**
 * Wraps the app with Cognito authentication.
 * If VITE_COGNITO_* env vars are not set, renders children directly (no auth).
 */
export default function AuthGate({ children }: Props) {
  const [authed, setAuthed] = useState(false);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    // If Cognito is not configured, skip auth entirely
    if (!isAuthConfigured()) {
      setAuthed(true);
      setLoading(false);
      return;
    }

    // Check for OAuth callback code in URL
    const params = new URLSearchParams(window.location.search);
    const code = params.get("code");

    if (code) {
      // Exchange code for tokens, then clean up URL
      exchangeCode(code).then((ok) => {
        if (ok) {
          window.history.replaceState({}, "", window.location.pathname);
          setAuthed(true);
        }
        setLoading(false);
      });
    } else if (isAuthenticated()) {
      setAuthed(true);
      setLoading(false);
    } else {
      setLoading(false);
    }
  }, []);

  if (loading) {
    return <div className="auth-loading">Loading...</div>;
  }

  if (!authed) {
    return (
      <div className="auth-login">
        <div className="auth-card">
          <h2>Fraud Detection &amp; Quality Analysis</h2>
          <p>Sign in to access the dashboard.</p>
          <a href={loginUrl()} className="auth-button">
            Sign in
          </a>
        </div>
      </div>
    );
  }

  return (
    <>
      {isAuthConfigured() && (
        <div className="auth-bar">
          <span>{getUserEmail()}</span>
          <button
            onClick={() => {
              clearTokens();
              window.location.href = logoutUrl();
            }}
          >
            Sign out
          </button>
        </div>
      )}
      {children}
    </>
  );
}
