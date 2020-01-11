FROM node:12

WORKDIR /opt/feeder

COPY . .

RUN npm install

EXPOSE 8081

CMD ["node", "feeder.node.js"]
