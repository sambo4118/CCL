import express from 'express';
import crypto from 'node:crypto';
import fs from 'node:fs';
import { exec } from 'node:child_process';
import { promisify } from 'node:util';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const execAsync = promisify(exec);
const router = express.Router();
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.resolve(__dirname, '..');

const TARGET_BRANCH = process.env.GIT_BRANCH || 'v2';
const TARGET_REF = `refs/heads/${TARGET_BRANCH}`;
let isUpdating = false;

function verifyGitHubSignature(req, secret) {
    const signatureHeader = req.get('x-hub-signature-256');
    if (!signatureHeader || !req.rawBody) {
        return false;
    }

    const expectedSignature = `sha256=${crypto
        .createHmac('sha256', secret)
        .update(req.rawBody)
        .digest('hex')}`;

    const sigBuffer = Buffer.from(signatureHeader, 'utf8');
    const expectedBuffer = Buffer.from(expectedSignature, 'utf8');

    if (sigBuffer.length !== expectedBuffer.length) {
        return false;
    }

    return crypto.timingSafeEqual(sigBuffer, expectedBuffer);
}

router.post(
    '/',
    express.urlencoded({
        extended: true,
        verify: (req, res, buf) => {
            req.rawBody = buf;
        }
    }),
    (req, res) => {
        const secret = process.env.SECRET_UPDATE;
        if (!secret) {
            console.error('[Update Webhook] SECRET_UPDATE is not configured in .env');
            return res.status(500).json({ error: 'Webhook secret not configured on server' });
        }

        if (!verifyGitHubSignature(req, secret)) {
            console.warn('[Update Webhook] Invalid or missing X-Hub-Signature-256 header');
            return res.status(401).json({ error: 'Invalid signature' });
        }

        const event = req.get('x-github-event');
        if (event === 'ping') {
            return res.status(200).json({ message: 'Pong' });
        }

        if (event && event !== 'push') {
            return res.status(200).json({ message: `Ignored event: ${event}` });
        }

        let payload = req.body;
        if (typeof req.body?.payload === 'string') {
            try {
                payload = JSON.parse(req.body.payload);
            } catch {
                return res.status(400).json({ error: 'Invalid JSON payload' });
            }
        }

        if (payload?.ref !== TARGET_REF) {
            return res.status(200).json({
                message: `Ignored push to ${payload?.ref || 'unknown ref'}, only updating on ${TARGET_REF}`
            });
        }

        if (isUpdating) {
            return res.status(202).json({ message: 'Update already in progress' });
        }

        isUpdating = true;

        res.on('finish', async () => {
            try {
                // In the Synology Docker container, entrypoint.sh handles git fetch/reset,
                // .env syncing, npm install/rebuild, and migrations on container restart.
                if (fs.existsSync('/usr/local/bin/entrypoint.sh')) {
                    console.log('[Update Webhook] Exiting container so Docker restarts and runs entrypoint.sh...');
                    process.exit(0);
                }

                console.log(`[Update Webhook] Pulling latest changes from origin/${TARGET_BRANCH}...`);
                const pullResult = await execAsync(
                    `git fetch origin ${TARGET_BRANCH} && git reset --hard origin/${TARGET_BRANCH}`,
                    { cwd: projectRoot }
                );
                if (pullResult.stdout) console.log(pullResult.stdout.trim());
                if (pullResult.stderr) console.error(pullResult.stderr.trim());

                console.log('[Update Webhook] Installing dependencies...');
                const installResult = await execAsync('npm install --no-package-lock', { cwd: projectRoot });
                if (installResult.stdout) console.log(installResult.stdout.trim());
                if (installResult.stderr) console.error(installResult.stderr.trim());

                console.log('[Update Webhook] Update complete. Exiting process for restart...');
                process.exit(0);
            } catch (err) {
                isUpdating = false;
                console.error('[Update Webhook] Update failed:', err);
            }
        });

        res.status(200).json({ message: `Update started for branch ${TARGET_BRANCH}` });
    }
);

export default router;
