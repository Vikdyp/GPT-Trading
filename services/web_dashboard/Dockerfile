FROM node:20-slim

WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install --no-progress --fund=false --audit=false || true

COPY . .
EXPOSE 3000
CMD ["npm", "run", "dev"]
