FROM node:18-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci --only=production

COPY generator.js .

CMD ["node", "generator.js"]