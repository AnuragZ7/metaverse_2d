FROM node:22.20.0-alpine AS build
WORKDIR /workspace/metaverse
RUN corepack enable
COPY metaverse/package.json metaverse/pnpm-lock.yaml metaverse/pnpm-workspace.yaml metaverse/turbo.json ./
COPY metaverse/apps/ws/package.json apps/ws/package.json
COPY metaverse/apps/http/package.json apps/http/package.json
COPY metaverse/apps/nexus-office-env/package.json apps/nexus-office-env/package.json
COPY metaverse/packages/database/package.json packages/database/package.json
COPY metaverse/packages/eslint-config/package.json packages/eslint-config/package.json
COPY metaverse/packages/typescript-config/package.json packages/typescript-config/package.json
COPY metaverse/packages/ui/package.json packages/ui/package.json
RUN pnpm install --frozen-lockfile --ignore-scripts
COPY metaverse/apps/ws apps/ws
RUN pnpm --filter ws build

FROM node:22.20.0-alpine AS runtime
ENV NODE_ENV=production
WORKDIR /workspace/metaverse
RUN addgroup -S skyscope && adduser -S skyscope -G skyscope
COPY --from=build --chown=skyscope:skyscope /workspace/metaverse /workspace/metaverse
USER skyscope
EXPOSE 8000
CMD ["node", "apps/ws/dist/index.js"]
