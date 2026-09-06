FROM node:22-bookworm-slim AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY tsconfig.json ./
COPY scripts/build.mjs scripts/build.mjs
COPY src src
COPY test test
RUN npm run build && npm prune --omit=dev --ignore-scripts --no-audit --no-fund

FROM node:22-bookworm-slim AS worker
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /data/homes \
    && chown node:node /data/homes
WORKDIR /app
ENV NODE_ENV=production \
    AGENT_OS_PYTHON=python3 \
    AGENT_OS_HOMES_ROOT=/data/homes \
    AGENT_OS_WORKER_PORT=5190
COPY --from=build /app/package.json ./
COPY --from=build /app/node_modules node_modules
COPY --from=build /app/dist/src dist/src
COPY db/schema.sql db/schema.sql
COPY kernel/runner.py kernel/runner.py
USER node
EXPOSE 5190
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:'+(process.env.AGENT_OS_WORKER_PORT||5190)+'/healthz').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
CMD ["node", "dist/src/worker/main.js"]
