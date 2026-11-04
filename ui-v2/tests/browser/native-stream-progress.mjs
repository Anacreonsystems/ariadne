/**
 * Browser checks of the native progress stories, using their MSW API fixtures.
 * Start Storybook first. These checks complement the real HTTP/PostgreSQL gate;
 * they do not establish server or process-recovery acceptance.
 */
import assert from "node:assert/strict";
import { mkdir, mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import AxeBuilder from "@axe-core/playwright";
import { chromium, expect } from "@playwright/test";

const storybookUrl =
	process.env.NATIVE_STORYBOOK_URL ?? "http://127.0.0.1:6008";
const outputDirectory =
	process.env.NATIVE_BROWSER_OUTPUT ??
	(await mkdtemp(join(tmpdir(), "ariadne-native-stream-progress-")));
await mkdir(outputDirectory, { recursive: true, mode: 0o700 });
const browser = await chromium.launch({
	...(process.env.CHROMIUM_EXECUTABLE_PATH
		? { executablePath: process.env.CHROMIUM_EXECUTABLE_PATH }
		: {}),
	args: ["--no-sandbox"],
});
const results = [];
let workerMessageSecurity;
const profiles = [
	{
		name: "desktop-light",
		viewport: { width: 1440, height: 1000 },
		theme: "Light",
	},
	{
		name: "desktop-dark",
		viewport: { width: 1440, height: 1000 },
		theme: "Dark",
	},
	{
		name: "mobile-light",
		viewport: { width: 390, height: 844 },
		theme: "Light",
	},
];
try {
	for (const profile of profiles) {
		for (const story of [
			"running",
			"draining",
			"cancelling",
			"awaiting-worker",
			"released",
			"unavailable",
			"loading",
		]) {
			const context = await browser.newContext({ viewport: profile.viewport });
			const page = await context.newPage();
			const errors = [];
			page.on("pageerror", (error) => errors.push(error.message));
			try {
				await page.goto(
					`${storybookUrl}/iframe.html?id=components-flowruns-nativestreamprogress--${story}&viewMode=story&globals=theme:${profile.theme}`,
				);
				await expect(
					page.getByRole("heading", { name: "Streaming progress" }),
				).toBeVisible();
				if (story === "loading")
					await expect(
						page.getByRole("status", { name: "Loading streaming progress" }),
					).toBeVisible();
				else if (story === "unavailable")
					await expect(page.getByRole("alert")).toContainText(
						"Streaming progress unavailable",
					);
				else if (story === "awaiting-worker")
					await expect(page.getByText("Generation not claimed")).toBeVisible();
				else if (story === "released") {
					await expect(
						page.getByText(
							/Consumer positions and finite receipts are no longer retained/,
						),
					).toBeVisible();
					await expect(
						page.getByText("Receipt no longer retained"),
					).toHaveCount(2);
					await expect(page.getByText("Awaiting receipt")).toHaveCount(0);
				} else {
					await expect(page.getByText("Generation 2")).toBeVisible();
					await expect(
						page.getByRole("link", { name: "documents.sign" }),
					).toHaveAttribute("href", /\/runs\/task-run\//);
					await expect(page.getByText("301", { exact: true })).toBeVisible();
					if (story === "draining")
						await expect(
							page.getByText("Source admission closed"),
						).toBeVisible();
					if (story === "cancelling")
						await expect(
							page.getByText("Cancellation requested"),
						).toBeVisible();
				}
				if (profile.name === "desktop-light" && story === "running") {
					workerMessageSecurity = await verifyWorkerMessageSecurity(
						page,
						context,
						new URL(storybookUrl).origin,
					);
				}
				await page.screenshot({
					path: `${outputDirectory}/${profile.name}-${story}.png`,
					fullPage: true,
				});
				const accessibility = await new AxeBuilder({ page })
					.include('[data-slot="card"]')
					.withTags(["wcag2a", "wcag2aa"])
					.analyze();
				assert.deepEqual(
					accessibility.violations.map(({ id, impact }) => ({ id, impact })),
					[],
					`${profile.name}/${story}: accessibility violations`,
				);
				assert.deepEqual(
					errors,
					[],
					`${profile.name}/${story}: browser errors`,
				);
				const overflow = await page.evaluate(
					() => document.documentElement.scrollWidth > window.innerWidth,
				);
				assert.equal(
					overflow,
					false,
					`${profile.name}/${story}: page overflow`,
				);
				results.push({
					profile: profile.name,
					story,
					browser_errors: errors.length,
					wcag_violations: accessibility.violations.length,
					page_overflow: overflow,
				});
			} finally {
				await context.close();
			}
		}
	}
	for (const profile of profiles) {
		for (const story of [
			"drain",
			"cancel",
			"awaiting-enrollment",
			"unavailable",
		]) {
			const context = await browser.newContext({ viewport: profile.viewport });
			const page = await context.newPage();
			const errors = [];
			page.on("pageerror", (error) => errors.push(error.message));
			try {
				await page.goto(
					`${storybookUrl}/iframe.html?id=components-flowruns-nativestreamcontroldialog--${story}&viewMode=story&globals=theme:${profile.theme}`,
				);
				await expect(page.getByRole("alertdialog")).toBeVisible();
				if (story === "unavailable")
					await expect(page.getByRole("alert")).toContainText(
						"Current streaming generation unavailable",
					);
				else if (story === "awaiting-enrollment")
					await expect(
						page.getByText("Awaiting worker enrollment."),
					).toBeVisible();
				else await expect(page.getByText("Generation 2")).toBeVisible();
				await page.screenshot({
					path: `${outputDirectory}/${profile.name}-control-${story}.png`,
					fullPage: true,
				});
				const accessibility = await new AxeBuilder({ page })
					.include('[role="alertdialog"]')
					.withTags(["wcag2a", "wcag2aa"])
					.analyze();
				assert.deepEqual(
					accessibility.violations.map(({ id, impact }) => ({ id, impact })),
					[],
					`${profile.name}/control-${story}: accessibility violations`,
				);
				assert.deepEqual(
					errors,
					[],
					`${profile.name}/control-${story}: browser errors`,
				);
				results.push({
					profile: profile.name,
					story: `control-${story}`,
					browser_errors: errors.length,
					wcag_violations: accessibility.violations.length,
				});
			} finally {
				await context.close();
			}
		}
	}

	await writeFile(
		`${outputDirectory}/results.json`,
		`${JSON.stringify({ scope: "Storybook browser/MSW fixtures", worker_message_security: workerMessageSecurity, results }, null, 2)}\n`,
	);
	console.log(
		JSON.stringify({
			cases: results.length,
			artifact_directory: outputDirectory,
			worker_message_security: workerMessageSecurity,
			results,
		}),
	);
} finally {
	await browser.close();
}

async function verifyWorkerMessageSecurity(page, context, expectedOrigin) {
	const worker = context.serviceWorkers()[0];
	assert.ok(worker, "The actual MSW service worker must be active");
	await worker.evaluate(() => {
		self.__nativeSecurityOrigins = [];
		self.addEventListener("message", (event) => {
			if (event.origin !== self.location.origin) return;
			if (event.data === "KEEPALIVE_REQUEST")
				self.__nativeSecurityOrigins.push(event.origin);
		});
	});
	await page.evaluate(() =>
		navigator.serviceWorker.controller.postMessage("KEEPALIVE_REQUEST"),
	);
	await expect
		.poll(() => worker.evaluate(() => self.__nativeSecurityOrigins))
		.toContain(expectedOrigin);
	const rejection = await worker.evaluate(async () => {
		const client = (await self.clients.matchAll({ type: "window" }))[0];
		if (!client) throw new Error("The controlled browser client is missing");
		const original = self.clients.get;
		let lookups = 0;
		self.clients.get = function (...args) {
			lookups += 1;
			return original.apply(this, args);
		};
		try {
			self.dispatchEvent(
				new ExtendableMessageEvent("message", {
					origin: "https://untrusted.invalid",
					data: "KEEPALIVE_REQUEST",
					source: client,
				}),
			);
			return {
				foreign_client_lookups: lookups,
				worker_origin: self.location.origin,
			};
		} finally {
			self.clients.get = original;
		}
	});
	assert.equal(
		rejection.foreign_client_lookups,
		0,
		"Foreign-origin messages must be rejected before client lookup",
	);
	assert.equal(rejection.worker_origin, expectedOrigin);
	return {
		...rejection,
		legitimate_message_origin: expectedOrigin,
		foreign_message_rejected: true,
	};
}
