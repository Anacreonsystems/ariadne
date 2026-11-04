import createClient, { type Middleware } from "openapi-fetch";
import { csrfMiddleware } from "./csrf";
import type { paths } from "./prefect.ts";
import { uiSettings } from "./ui-settings";

const AUTH_STORAGE_KEY = "prefect-password";

// Event for auth state changes (logout on 401)
const authErrorEvent = new CustomEvent("auth:unauthorized");

const handleUnauthorized: Middleware = {
	onResponse({ response }) {
		if (response.status === 401) {
			// Clear stored credentials
			localStorage.removeItem(AUTH_STORAGE_KEY);
			// Dispatch event for UI to handle redirect
			window.dispatchEvent(authErrorEvent);
		}
	},
};

const throwOnError: Middleware = {
	async onResponse({ response }) {
		if (!response.ok) {
			let message = `API request failed with status ${response.status}`;
			try {
				const body: unknown = await response.clone().json();
				if (
					body &&
					typeof body === "object" &&
					!Array.isArray(body) &&
					"detail" in body
				) {
					const detail = body.detail;
					if (typeof detail === "string") message = detail;
					else if (
						detail &&
						typeof detail === "object" &&
						!Array.isArray(detail) &&
						"code" in detail &&
						"message" in detail &&
						typeof detail.code === "string" &&
						/^[A-Z][A-Z0-9_]{0,63}$/.test(detail.code) &&
						typeof detail.message === "string"
					) {
						message = `${detail.code}: ${detail.message}`;
					}
				}
			} catch {
				// Opaque errors use status only; never serialize validation input.
			}
			throw new Error(message);
		}
	},
};

const authMiddleware: Middleware = {
	onRequest({ request }) {
		const password = localStorage.getItem(AUTH_STORAGE_KEY);
		if (password) {
			request.headers.set("Authorization", `Basic ${password}`);
		}
		return request;
	},
};

let client: ReturnType<typeof createClient<paths>> | null = null;
let clientBaseUrl: string | null = null;

/**
 * Get the API query service client.
 * On first call, fetches the API URL from /ui-settings endpoint.
 * Subsequent calls return the cached client.
 */
export const getQueryService = async () => {
	const apiUrl = await uiSettings.getApiUrl();

	// Create new client if URL changed or not initialized
	if (!client || clientBaseUrl !== apiUrl) {
		client = createClient<paths>({
			baseUrl: apiUrl,
		});
		client.use(csrfMiddleware);
		client.use(authMiddleware);
		client.use(handleUnauthorized);
		client.use(throwOnError);
		clientBaseUrl = apiUrl;
	}

	return client;
};

/**
 * Get API URL for use in raw fetch calls.
 * Prefer using getQueryService() when possible.
 */
export const getApiUrl = async (): Promise<string> => {
	return uiSettings.getApiUrl();
};
