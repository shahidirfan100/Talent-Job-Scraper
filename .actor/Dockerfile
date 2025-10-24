FROM apify/actor-node:22

COPY package*.json ./

RUN npm install --only=prod --no-optional

COPY . ./

CMD npm start
