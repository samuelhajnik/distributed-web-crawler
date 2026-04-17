# Monorepo root must be the build context (see docker-compose.yml).
FROM node:22-alpine AS build
WORKDIR /app

COPY package.json package-lock.json tsconfig.base.json ./
COPY packages ./packages
COPY services ./services

RUN npm ci

RUN npm run build -w @crawler/shared && npm run build -w control-plane

RUN test -f packages/shared/dist/index.js \
  && test -f services/control-plane/dist/index.js

FROM node:22-alpine
WORKDIR /app
ENV NODE_ENV=production

COPY --from=build /app ./

RUN npm prune --omit=dev

RUN test -f services/control-plane/dist/index.js

CMD ["node", "services/control-plane/dist/index.js"]
