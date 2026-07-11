FROM nginx:alpine
COPY index.html /usr/share/nginx/html/index.html
COPY jobs.html /usr/share/nginx/html/jobs.html
COPY mobile.css /usr/share/nginx/html/mobile.css
RUN sed -i 's#</head>#  <link rel="stylesheet" href="/mobile.css" />\n  </head>#' /usr/share/nginx/html/index.html
EXPOSE 80
