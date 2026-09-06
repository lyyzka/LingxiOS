import assert from 'node:assert/strict'
import { it } from 'node:test'
import { renameDocument } from '../dist/src/integrations/lingxiloop/document-writes.js'

it('renames with native SQL under the human permission transaction and reports publication failure', async () => {
    const { PGlite } = await import('@electric-sql/pglite');
    const { readFile } = await import('node:fs/promises');
    const { createRequire } = await import('node:module');
    const { resolve } = await import('node:path');
    const { fileURLToPath, pathToFileURL } = await import('node:url');
    const ts = (await import('typescript')).default;
    const source = resolve(process.argv[2] ?? process.env['LINGXILOOP_SOURCE'] ?? fileURLToPath(new URL('../../LingxiLoop/server/src', import.meta.url)));
    const nativeRequire = createRequire(resolve(source, '../package.json'));
    const repository = ts.transpileModule(await readFile(resolve(source, 'modules/documents/repository.ts'), 'utf8'), { compilerOptions: { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.ESNext } }).outputText;
    const native = await import('data:text/javascript;base64,' + Buffer.from(repository).toString('base64'));
    const schemas = ts.transpileModule(await readFile(resolve(source, 'modules/documents/contracts.ts'), 'utf8'), { compilerOptions: { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.ESNext } }).outputText
        .replace("'zod'", JSON.stringify(pathToFileURL(nativeRequire.resolve('zod')).href));
    const { renameDocumentRequestSchema } = await import('data:text/javascript;base64,' + Buffer.from(schemas).toString('base64'));
    const connectionString = process.env.LINGXIOS_DOCUMENTS_TEST_DATABASE_URL;
    const postgres = connectionString ? new (nativeRequire('pg').Pool)({ connectionString, max: 4, connectionTimeoutMillis: 5000 }) : null;
    const db = postgres ? { query: (...args) => postgres.query(...args), exec: sql => postgres.query(sql), close: () => postgres.end() } : new PGlite();
    const pool = { query: async (sql, args) => {
            const value = await db.query(sql, args ? [...args] : undefined);
            return { rows: value.rows, rowCount: value.rowCount ?? value.affectedRows ?? value.rows.length };
        }, connect: async () => {
            if (!postgres) return { query: pool.query, release() {} };
            const client = await postgres.connect();
            return { query: client.query.bind(client), release: () => client.release() };
        } };
    let denied = false;
    let falseAcknowledgement = false;
    let event;
    const services = { documents: {
            listAgentDocuments: async () => [], readAgentDocument: async () => { throw new Error('unused'); },
            writes: {
                createPermissionService: (client, options) => {
                    assert.equal(typeof client.query, 'function');
                    assert.deepEqual(options, { lockDependencies: true });
                    return { assertCan: async (input) => { assert.equal(input.actorUserId, 'human'); if (denied)
                            throw new Error('denied'); } };
                },
                renameDocument: async (...args) => { const result = await native.renameDocument(...args); return falseAcknowledgement ? false : result; },
                renameDocumentRequestSchema, CH_DOCS: 'native-docs', publish: async (channel, value) => { assert.equal(channel, 'native-docs'); event = value; throw new Error('transport failed'); },
            },
        } };
    const work = { id: 'w', tenantId: 't', agentId: 'agent', principalId: 'human', sessionId: 'room', kind: 'turn', lane: 'interactive', triggerRef: 'message', fence: 1, homeEpoch: 1, leaseToken: 'token' };
    const action = { runId: 'w', cellId: 'c', callIndex: 0, idempotencyKey: 'key', action: 'documents.rename', args: { documentId: 'doc', expectedTitle: 'Old', title: 'New' } };
    try {
        const existing = await db.query("SELECT tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema')");
        assert.equal(existing.rows.length, 0, 'document checks require an empty disposable database');
        await db.exec("CREATE TABLE conversations(id text,company_id text,project_id text); INSERT INTO conversations VALUES('room','t','p'); CREATE TABLE documents(id text,company_id text,project_id text,title text,updated_at timestamptz,conversation_id text); INSERT INTO documents VALUES('doc','t','p','Old',NOW(),'room')");
        assert.deepEqual(await renameDocument(pool, services, work, action), { documentId: 'doc', title: 'New', notification: 'unconfirmed' });
        assert.deepEqual(event, { type: 'doc.changed', kind: 'document.updated', companyId: 't', workspaceId: 'p', documentId: 'doc', actorId: 'agent' });
        await assert.rejects(renameDocument(pool, services, work, action), /title changed/);
        denied = true;
        await assert.rejects(renameDocument(pool, services, work, { ...action, args: { ...action.args, expectedTitle: 'New', title: 'Denied' } }), /denied/);
        denied = false;
        falseAcknowledgement = true;
        await assert.rejects(renameDocument(pool, services, work, { ...action, args: { ...action.args, expectedTitle: 'New', title: 'Rolled back' } }), /did not update/);
        assert.deepEqual((await db.query('SELECT title FROM documents')).rows, [{ title: 'New' }]);
        // Load the actual permission factory and its runtime imports without changing policy or SQL.
        const modules = new Map();
        async function loadNative(file) {
            if (modules.has(file)) return modules.get(file);
            let code = ts.transpileModule(await readFile(file, 'utf8'), { compilerOptions: { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.ESNext } }).outputText;
            for (const match of [...code.matchAll(/from ['"]([^'"]+)['"]/g)]) {
                const dependency = match[1];
                if (!dependency.startsWith('.')) throw new Error('unexpected native dependency: ' + dependency);
                const url = await loadNative(resolve(file, '..', dependency.replace(/\.js$/, '.ts')));
                code = code.replace(match[0], 'from ' + JSON.stringify(url));
            }
            const url = 'data:text/javascript;base64,' + Buffer.from(code).toString('base64');
            modules.set(file, url);
            return url;
        }
        const { createPermissionService } = await import(await loadNative(resolve(source, 'modules/access/public.ts')));
        await db.exec(`
            ALTER TABLE conversations ADD COLUMN members jsonb DEFAULT '["human"]', ADD COLUMN leader_id text;
            ALTER TABLE documents ADD COLUMN created_by text DEFAULT 'agent';
            CREATE TABLE users(id text PRIMARY KEY,email text,email_verified_at timestamptz,deleted_at timestamptz,suspended_at timestamptz);
            INSERT INTO users(id) VALUES('human');
            CREATE TABLE companies(id text PRIMARY KEY,type text,status text,plan_id text);
            INSERT INTO companies VALUES('t','PERSONAL','ACTIVE','plan');
            CREATE TABLE projects(id text PRIMARY KEY,company_id text,kind text,plan_id text,status text);
            INSERT INTO projects VALUES('p','t','PERSONAL_LEARNING',NULL,'ACTIVE');
            CREATE TABLE company_memberships(company_id text,user_id text,role text,status text);
            INSERT INTO company_memberships VALUES('t','human','OWNER','ACTIVE');
            CREATE TABLE project_memberships(company_id text,project_id text,user_id text,role text,status text);
            INSERT INTO project_memberships VALUES('t','p','human','OWNER','ACTIVE');
            CREATE TABLE plans(id text PRIMARY KEY,code text,status text);
            INSERT INTO plans VALUES('plan','test','ACTIVE');
            CREATE TABLE entitlements(id text PRIMARY KEY,code text);
            INSERT INTO entitlements VALUES('conversation','conversation.core');
            CREATE TABLE plan_entitlements(plan_id text,entitlement_id text,value jsonb);
            INSERT INTO plan_entitlements VALUES('plan','conversation','true');
        `);
        services.documents.writes.createPermissionService = createPermissionService;
        falseAcknowledgement = false;
        const authorized = { ...action, args: { ...action.args, expectedTitle: 'New', title: 'Authorized' } };
        assert.deepEqual(await renameDocument(pool, services, work, authorized), { documentId: 'doc', title: 'Authorized', notification: 'unconfirmed' });
        const next = { ...action, args: { ...action.args, expectedTitle: 'Authorized', title: 'Forbidden' } };
        for (const [revoke, restore, reason] of [
            ["UPDATE project_memberships SET status='INACTIVE'", "UPDATE project_memberships SET status='ACTIVE'", 'PROJECT_MEMBERSHIP_INACTIVE'],
            ["UPDATE conversations SET members='[]'", "UPDATE conversations SET members='[\"human\"]'", 'RESOURCE_MEMBERSHIP_REQUIRED'],
            ["UPDATE plan_entitlements SET value='false'", "UPDATE plan_entitlements SET value='true'", 'ENTITLEMENT_MISSING'],
            ["UPDATE project_memberships SET role='OBSERVER'", "UPDATE project_memberships SET role='OWNER'", 'ROLE_NOT_ALLOWED'],
            ["UPDATE users SET suspended_at=NOW()", "UPDATE users SET suspended_at=NULL", 'ACTOR_INACTIVE'],
        ]) {
            await db.exec(revoke);
            await assert.rejects(renameDocument(pool, services, work, next), error => error.reason === reason);
            assert.deepEqual((await db.query('SELECT title FROM documents')).rows, [{ title: 'Authorized' }]);
            await db.exec(restore);
        }

        if (postgres) {
            await db.exec("INSERT INTO conversations VALUES('linked','t','p','[\"human\"]',NULL); UPDATE documents SET conversation_id='linked'");
            const revoker = await postgres.connect();
            try {
                await revoker.query("SET lock_timeout='200ms'");
                services.documents.writes.renameDocument = async (...args) => {
                    // This runs after authorization but before the native write, on a second connection.
                    await assert.rejects(revoker.query("UPDATE conversations SET members='[]' WHERE id='linked'"), error => error.code === '55P03');
                    return native.renameDocument(...args);
                };
                assert.equal((await renameDocument(pool, services, work, next)).title, 'Forbidden');
                await revoker.query("UPDATE conversations SET members='[]' WHERE id='linked'");
                await assert.rejects(renameDocument(pool, services, work, { ...next, args: { ...next.args, expectedTitle: 'Forbidden', title: 'After revocation' } }), error => error.reason === 'RESOURCE_MEMBERSHIP_REQUIRED');
                assert.deepEqual((await db.query('SELECT title FROM documents')).rows, [{ title: 'Forbidden' }]);
            } finally { revoker.release(); }
        }
        await assert.rejects(renameDocument(pool, services, work, { ...action, args: { ...action.args, title: '' } }));
    }
    finally {
        await db.close();
    }
});
export {};
