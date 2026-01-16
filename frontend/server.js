const http = require("http");
const fs = require("fs");
const path = require("path");

const port = process.env.PORT || 5173;
const publicDir = __dirname;

const contentTypes = {
  ".html": "text/html",
  ".js": "text/javascript",
  ".css": "text/css",
  ".json": "application/json",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".svg": "image/svg+xml",
};

const server = http.createServer((req, res) => {
  const requestPath = req.url === "/" ? "/index.html" : req.url;
  const filePath = path.join(publicDir, requestPath);

  fs.readFile(filePath, (error, content) => {
    if (error) {
      if (error.code === "ENOENT") {
        res.writeHead(404, { "Content-Type": "text/plain" });
        res.end("Not found");
      } else {
        res.writeHead(500, { "Content-Type": "text/plain" });
        res.end("Server error");
      }
      return;
    }

    const extname = path.extname(filePath);
    const contentType = contentTypes[extname] || "application/octet-stream";
    res.writeHead(200, { "Content-Type": contentType });
    res.end(content);
  });
});

server.listen(port, () => {
  // eslint-disable-next-line no-console
  console.log(`Frontend running at http://localhost:${port}`);
});
