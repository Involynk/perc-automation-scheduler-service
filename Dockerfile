# ==============================================================================
# Stage 1: Build stage
# ==============================================================================
FROM node:22-alpine AS builder

WORKDIR /app

# node:22-alpine already ships with OpenSSL (required by Node.js itself).
# No apk install needed.

COPY package*.json ./
COPY prisma ./prisma/

RUN npm ci

COPY . .

# Generate Prisma Client and build NestJS production bundle
RUN npx prisma generate
RUN npm run build

# Prune dev dependencies for production
RUN npm prune --production

# ==============================================================================
# Stage 2: Production runtime stage
# ==============================================================================
FROM node:22-alpine AS runner

WORKDIR /app

# node:22-alpine already ships with OpenSSL. No apk install needed.

ENV NODE_ENV=production

COPY package*.json ./
COPY prisma ./prisma/
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/dist ./dist

EXPOSE 3000

# Run migrations then start application
CMD ["sh", "-c", "npx prisma migrate deploy && node dist/main"]
