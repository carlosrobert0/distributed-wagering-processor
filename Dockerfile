FROM oven/bun:1.4.4 AS deps
WORKDIR /app
COPY package.json bun.lock* ./
RUN bun install --production --frozen-lockfile

FROM oven/bun:1.4.4
WORKDIR /app
ENV NODE_ENV=production
COPY --from=deps /app/node_modules ./node_modules
COPY package.json tsconfig.json ./
COPY src ./src
USER bun
EXPOSE 3000
CMD ["bun", "run", "src/main.ts"]
