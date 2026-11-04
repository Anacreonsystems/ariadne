/** Read the packaged UI from an owned real server; no mocked responses. */
import assert from "node:assert/strict";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { chromium, expect } from "@playwright/test";

const handoff = process.env.NATIVE_UI_HANDOFF
	? JSON.parse(await readFile(process.env.NATIVE_UI_HANDOFF, "utf8"))
	: {};
const flowUrl = process.env.NATIVE_UI_FLOW_URL ?? handoff.flow_url;
const apiAuth = process.env.NATIVE_UI_API_AUTH ?? handoff.api_auth;
assert.equal(typeof flowUrl, "string", "A FlowRun page URL is required");
assert.equal(typeof apiAuth, "string", "Basic API authentication is required");
const url = new URL(flowUrl);
const flowRunId = url.pathname.split("/").at(-1);
assert.match(flowRunId, /^[0-9a-f-]{36}$/i);
const separator = apiAuth.indexOf(":");
assert.ok(separator > 0);
const expectedCount = handoff.expected_count ?? 8;
const output =
	process.env.NATIVE_BROWSER_OUTPUT ?? "/tmp/native-stage9-ui-live";
await mkdir(output, { recursive: true });
const browser = await chromium.launch({
	...(process.env.CHROMIUM_EXECUTABLE_PATH
		? { executablePath: process.env.CHROMIUM_EXECUTABLE_PATH }
		: {}),
	args: [
		"--no-sandbox",
		"--disable-dev-shm-usage",
		"--enable-unsafe-swiftshader",
	],
});
const context = await browser.newContext({
	viewport: { width: 1440, height: 1200 },
	httpCredentials: {
		origin: url.origin,
		username: apiAuth.slice(0, separator),
		password: apiAuth.slice(separator + 1),
	},
});
await context.addInitScript(
	({ origin, encoded }) => {
		if (location.origin === origin)
			localStorage.setItem("prefect-password", encoded);
	},
	{ origin: url.origin, encoded: Buffer.from(apiAuth).toString("base64") },
);
const page = await context.newPage();
const errors = [];
page.on("pageerror", (error) => errors.push(error.message));
try {
	const responsePromise = page.waitForResponse(
		(response) =>
			new URL(response.url()).pathname ===
			`/api/streams/v1/deployments/${flowRunId}/projection`,
		{ timeout: 60_000 },
	);
	const documentResponse = await page.goto(flowUrl);
	assert.equal(documentResponse.status(), 200);
	const response = await responsePromise;
	assert.equal(response.status(), 200);
	assert.equal(response.fromServiceWorker(), false);
	const projection = await response.json();
	assert.equal(projection.flow_run_id, flowRunId);
	assert.equal(projection.generation, 1);
	assert.equal(projection.authority_state, "RELEASED");
	assert.equal(projection.flow_state, "COMPLETED");
	assert.equal(projection.items.length, 2);
	assert.equal(projection.finite_items.length, 2);
	assert.deepEqual(projection.items.map((item) => item.role).sort(), [
		"OPERATOR",
		"SOURCE",
	]);
	for (const item of projection.items) {
		assert.equal(item.last_sequence, expectedCount);
		assert.equal(item.record_count, 0);
		assert.equal(item.stream_state, "COMPLETED");
	}
	const forbidden = new Set([
		"value",
		"payload",
		"parameters",
		"holder_capability",
		"event_private_key",
		"child_process_id",
		"process_start_ticks",
		"host_id",
	]);
	function checkSafe(value) {
		if (!value || typeof value !== "object") return;
		for (const [key, child] of Object.entries(value)) {
			assert.equal(
				forbidden.has(key),
				false,
				`Private projection field: ${key}`,
			);
			checkSafe(child);
		}
	}
	checkSafe(projection);
	await expect(
		page.getByRole("heading", { name: "Streaming progress" }),
	).toBeVisible();
	await expect(page.getByText("Generation 1", { exact: true })).toBeVisible();
	await expect(
		page.getByText(
			/Consumer positions and finite receipts are no longer retained/,
		),
	).toBeVisible();
	await expect(
		page.getByText("Receipt no longer retained", { exact: true }),
	).toHaveCount(2);
	await expect(page.getByText("Awaiting receipt", { exact: true })).toHaveCount(
		0,
	);
	const links = [];
	for (const item of [...projection.items, ...projection.finite_items]) {
		const link = page.getByRole("link", {
			name: item.submission_key,
			exact: true,
		});
		await expect(link).toHaveAttribute(
			"href",
			new RegExp(`/runs/task-run/${item.task_run_id}$`),
		);
		links.push({
			submission_key: item.submission_key,
			task_run_id: item.task_run_id,
			href: await link.getAttribute("href"),
		});
	}
	const body = await page.locator("body").innerText();
	assert.equal(body.includes(apiAuth), false);
	assert.equal(body.includes(Buffer.from(apiAuth).toString("base64")), false);
	assert.equal(context.serviceWorkers().length, 0);
	await page.screenshot({ path: `${output}/flow-run.png`, fullPage: true });
	const operator = projection.items.find((item) => item.role === "OPERATOR");
	await page
		.getByRole("link", { name: operator.submission_key, exact: true })
		.click();
	await expect(page).toHaveURL((actual) =>
		actual.pathname.endsWith(`/runs/task-run/${operator.task_run_id}`),
	);
	await expect(
		page.getByRole("heading", { name: "Streaming progress" }),
	).toHaveCount(0);
	assert.deepEqual(errors, []);
	const result = {
		scope:
			"Packaged React UI on the real authenticated API/PostgreSQL worker fixture; no MSW or response mocks",
		flow_run_id: flowRunId,
		flow_page_status: documentResponse.status(),
		projection_status: response.status(),
		server_identity: response.headers()["x-native-recovery-id"] ?? null,
		browser: browser.version(),
		generation: projection.generation,
		flow_state: projection.flow_state,
		authority_state: projection.authority_state,
		published_positions: projection.items.map((item) => ({
			role: item.role,
			last_sequence: item.last_sequence,
		})),
		task_links: links,
		operator_navigation: "PASS",
		page_errors: errors,
		service_workers: context.serviceWorkers().length,
	};
	await writeFile(
		`${output}/results.json`,
		`${JSON.stringify(result, null, 2)}\n`,
	);
	console.log(JSON.stringify(result));
} finally {
	await browser.close();
}
