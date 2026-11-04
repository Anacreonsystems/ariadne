import { buildApiUrl, server } from "@tests/utils";
import { HttpResponse, http } from "msw";
import { describe, expect, it } from "vitest";
import { getQueryService } from "./service";

describe("API error response messages", () => {
	it.each([
		{
			name: "native capacity",
			status: 409,
			detail: {
				code: "CAPACITY",
				message: "Native deployment capacity reached",
			},
			expected: "CAPACITY: Native deployment capacity reached",
		},
		{
			name: "ordinary string",
			status: 404,
			detail: "Deployment not found",
			expected: "Deployment not found",
		},
		{
			name: "validation array",
			status: 422,
			detail: [
				{
					loc: ["body", "parameters"],
					msg: "Invalid input",
					input: "private-client-input",
				},
			],
			expected: "API request failed with status 422",
		},
		{
			name: "malformed object",
			status: 400,
			detail: { message: { input: "private-client-input" }, code: 123 },
			expected: "API request failed with status 400",
		},
		{
			name: "missing detail property",
			status: 500,
			detail: undefined,
			expected: "",
		},
		{
			name: "null detail",
			status: 500,
			detail: null,
			expected: "API request failed with status 500",
		},
	])(
		"formats $name without serializing validation input",
		async ({ status, detail, expected }) => {
			server.use(
				http.post(buildApiUrl("/deployments/:id/create_flow_run"), () =>
					HttpResponse.json({ detail }, { status }),
				),
			);
			const client = await getQueryService();
			await expect(
				client.POST("/deployments/{id}/create_flow_run", {
					params: { path: { id: "deployment-1" } },
					body: { parameters: {} },
				}),
			).rejects.toMatchObject({ message: expected });
		},
	);

	it("handles a non-JSON upstream failure without echoing the response body", async () => {
		server.use(
			http.post(buildApiUrl("/deployments/:id/create_flow_run"), () =>
				HttpResponse.text("private-upstream-debug-data", { status: 502 }),
			),
		);
		const client = await getQueryService();
		await expect(
			client.POST("/deployments/{id}/create_flow_run", {
				params: { path: { id: "deployment-1" } },
				body: { parameters: {} },
			}),
		).rejects.toMatchObject({ message: "API request failed with status 502" });
	});
});
