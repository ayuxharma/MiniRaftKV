# Node 24 is used only for the small dashboard gateway.
FROM node:24-bookworm-slim

WORKDIR /app/web

# Install dependencies before copying source files so Docker can cache this step.
COPY web/package.json web/package-lock.json ./

RUN npm ci --omit=dev --no-audit --no-fund

COPY web/server.js ./server.js
COPY web/public ./public

# server.js reads the same Protocol Buffer definition as the C++ nodes.
COPY proto /app/proto

EXPOSE 8080

# The dashboard does not need root privileges.
USER node

CMD ["npm", "start"]
