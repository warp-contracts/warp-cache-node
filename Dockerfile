FROM node:18.12.1
ENV NODE_ENV=production

WORKDIR /app
COPY ["package.json", "yarn.lock", "./"]
RUN yarn install --frozen-lockfile
COPY . .
RUN mv .env.defaults .env

# Save git commit hash
RUN echo $(git rev-parse HEAD) > GIT_HASH
RUN rm -rf .git/

VOLUME /app/sqlite
VOLUME /app/cache

CMD ["node", "src/listener.js"]
