import React, {
  createContext,
  useContext,
  useState,
  useCallback,
  useEffect,
  useRef,
} from "react";
// @ts-ignore -- keycloak-js types use "exports" field; bundler resolves the JS fine
import Keycloak from "keycloak-js";

interface AuthUser {
  username: string;
  roles: string[];
  groups: string[];
  email?: string;
}

interface AuthContextValue {
  user: AuthUser | null;
  isAuthenticated: boolean;
  isAuthEnabled: boolean;
  isInitializing: boolean;
  logout: () => void;
  keycloak: Keycloak | null;
}

const AuthContext = createContext<AuthContextValue>({
  user: null,
  isAuthenticated: false,
  isAuthEnabled: false,
  isInitializing: true,
  logout: () => {},
  keycloak: null,
});

interface ServerAuthConfig {
  auth_type: string;
  url?: string;
  realm?: string;
  client_id?: string;
  auth_discovery_url?: string;
  // Used by "kubernetes" auth: paths the browser is sent to in order to
  // (re)authenticate and to sign out, served by the authenticating proxy
  // (e.g. OpenShift oauth-proxy) that fronts the UI.
  login_path?: string;
  logout_path?: string;
}

function readServerAuthConfig(): ServerAuthConfig | null {
  // Injected by the Feast UI server (feast ui) into index.html from feature_store.yaml
  const el = document.getElementById("feast-auth-config");
  if (el) {
    try {
      return JSON.parse(el.textContent || "{}");
    } catch {
      /* fall through */
    }
  }

  // Dev mode fallback: fetch from /api/auth-config (served by setupProxy or REST server)
  return null;
}

function extractUser(kc: Keycloak): AuthUser {
  const parsed = kc.tokenParsed as any;
  const clientRoles: string[] =
    parsed?.resource_access?.[kc.clientId!]?.roles || [];
  const realmRoles: string[] = parsed?.realm_access?.roles || [];
  const combined = [...clientRoles, ...realmRoles];

  return {
    username: parsed?.preferred_username || "unknown",
    roles: combined.filter((v, i) => combined.indexOf(v) === i),
    groups: parsed?.groups || [],
    email: parsed?.email,
  };
}

const TOKEN_REFRESH_INTERVAL = 30_000;
const MIN_VALIDITY_SECS = 60;

const AuthProvider: React.FC<{ children: React.ReactNode }> = ({
  children,
}) => {
  const [kc, setKc] = useState<Keycloak | null>(null);
  const [user, setUser] = useState<AuthUser | null>(null);
  const [isInitializing, setIsInitializing] = useState(true);
  const [isAuthEnabled, setIsAuthEnabled] = useState(false);
  // Non-null when auth_type is "kubernetes": the UI is fronted by an
  // authenticating proxy that owns the login/session, so the SPA never holds a
  // token — it only needs the paths to (re)authenticate and sign out.
  const [kubeAuth, setKubeAuth] = useState<{
    loginPath: string;
    logoutPath: string;
  } | null>(null);
  const initStarted = useRef(false);

  const syncUser = useCallback((keycloak: Keycloak) => {
    if (keycloak.authenticated) {
      setUser(extractUser(keycloak));
    }
  }, []);

  const doRefresh = useCallback(
    (keycloak: Keycloak) => {
      keycloak
        .updateToken(MIN_VALIDITY_SECS)
        .then((refreshed: boolean) => {
          if (refreshed) {
            syncUser(keycloak);
          }
        })
        .catch(() => {
          console.warn("Token refresh failed — redirecting to login");
          keycloak.login();
        });
    },
    [syncUser],
  );

  useEffect(() => {
    if (initStarted.current) return;
    initStarted.current = true;

    const initAuth = async () => {
      const config = readServerAuthConfig();

      if (config && config.auth_type === "kubernetes") {
        // The UI runs behind an authenticating proxy (e.g. OpenShift
        // oauth-proxy) that performs the login and injects the user's token on
        // upstream /api requests. The SPA does not construct Keycloak or hold a
        // token — it just records where to (re)authenticate and sign out, and
        // renders immediately.
        setKubeAuth({
          loginPath: config.login_path || "/oauth/start",
          logoutPath: config.logout_path || "/oauth/sign_out",
        });
        setIsAuthEnabled(true);
        setIsInitializing(false);
        return;
      }

      if (!config || config.auth_type !== "oidc") {
        // Auth is disabled — skip Keycloak, render the app immediately
        setIsAuthEnabled(false);
        setIsInitializing(false);
        return;
      }

      setIsAuthEnabled(true);

      const keycloak = new Keycloak({
        url: config.url || "http://localhost:8080",
        realm: config.realm || "feast",
        clientId: config.client_id || "feast-ui",
      });

      keycloak.onTokenExpired = () => {
        console.info("Access token expired — attempting refresh");
        doRefresh(keycloak);
      };

      keycloak.onAuthRefreshSuccess = () => {
        syncUser(keycloak);
      };

      keycloak.onAuthRefreshError = () => {
        console.warn("Auth refresh error — redirecting to login");
        keycloak.login();
      };

      try {
        const authenticated = await keycloak.init({
          onLoad: "login-required",
          checkLoginIframe: false,
          pkceMethod: "S256",
        });

        setKc(keycloak);
        if (authenticated) {
          syncUser(keycloak);
        }
      } catch (err) {
        console.error("Keycloak init failed:", err);
      }

      setIsInitializing(false);
    };

    initAuth();
  }, [doRefresh, syncUser]);

  // Proactive token refresh
  useEffect(() => {
    if (!kc?.authenticated) return;
    const id = setInterval(() => doRefresh(kc), TOKEN_REFRESH_INTERVAL);
    return () => clearInterval(id);
  }, [kc, doRefresh]);

  // Global fetch interceptor for /api/ calls:
  //  - OIDC mode (kc): attach the bearer token and refresh+retry on 401.
  //  - kubernetes mode (kubeAuth): the proxy injects the token, so send the
  //    request unmodified; a 401 means the proxy session expired, so bounce
  //    the browser through the proxy login.
  useEffect(() => {
    const isKube = kubeAuth !== null;
    if (!kc && !isKube) return;

    const originalFetch = window.fetch;
    window.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof Request
            ? input.url
            : input.toString();

      const isApiCall = url.includes("/api/");

      if (isApiCall && kc && kc.authenticated && kc.token) {
        const mergedHeaders = new Headers(init?.headers);
        if (!mergedHeaders.has("Authorization")) {
          mergedHeaders.set("Authorization", `Bearer ${kc.token}`);
        }
        init = { ...init, headers: mergedHeaders };
      }

      let response = await originalFetch(input, init);

      if (response.status === 401 && isApiCall) {
        if (kc) {
          console.warn("API returned 401 — attempting token refresh and retry");
          try {
            await kc.updateToken(5);
            syncUser(kc);
            const retryHeaders = new Headers(init?.headers);
            retryHeaders.set("Authorization", `Bearer ${kc.token}`);
            response = await originalFetch(input, {
              ...init,
              headers: retryHeaders,
            });
          } catch {
            kc.login();
          }
        } else if (kubeAuth) {
          console.warn("API returned 401 — redirecting to proxy login");
          window.location.href = kubeAuth.loginPath;
        }
      }

      return response;
    };

    return () => {
      window.fetch = originalFetch;
    };
  }, [kc, kubeAuth, syncUser]);

  const logout = useCallback(() => {
    if (kc) {
      kc.logout({ redirectUri: window.location.origin });
    } else if (kubeAuth) {
      // Delegate sign-out to the proxy; it clears the session cookie and
      // redirects back to the IdP/login.
      window.location.href = kubeAuth.logoutPath;
    }
  }, [kc, kubeAuth]);

  const value: AuthContextValue = {
    user,
    // In kubernetes mode the proxy only forwards authenticated requests, so by
    // the time the SPA renders the user is authenticated.
    isAuthenticated: !!kc?.authenticated || kubeAuth !== null,
    isAuthEnabled,
    isInitializing,
    logout,
    keycloak: kc,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
};

const useAuth = () => useContext(AuthContext);

export default AuthContext;
export { AuthProvider, useAuth };
export type { AuthUser };
