const zipSessions = new Map();

function contentDisposition(filename) {
    const fallback = filename.replace(/[^\x20-\x7E]/g, "_");
    return "attachment; filename=\"" + fallback.replace(/["\\]/g, "_") + "\"; filename*=UTF-8''" + encodeURIComponent(filename);
}

self.addEventListener("message", function (event) {
    const data = event.data || {};
    if (data.type !== "zip-download-init" || !event.ports || !event.ports[0]) {
        return;
    }

    const id = String(data.id || "");
    const filename = String(data.filename || "archive.zip");
    if (!id) {
        event.ports[0].postMessage({ok: false, error: "Invalid download id"});
        return;
    }

    const port = event.ports[0];
    let isClosed = false;

    const stream = new ReadableStream({
        start(controller) {
            port.onmessage = function (msgEvent) {
                const msg = msgEvent.data || {};
                if (isClosed) {
                    return;
                }
                if (msg.type === "chunk" && msg.data) {
                    controller.enqueue(new Uint8Array(msg.data));
                    return;
                }
                if (msg.type === "end") {
                    isClosed = true;
                    controller.close();
                    zipSessions.delete(id);
                    port.close();
                    return;
                }
                if (msg.type === "abort") {
                    isClosed = true;
                    controller.error(new Error("ZIP stream aborted"));
                    zipSessions.delete(id);
                    port.close();
                }
            };
        },
        cancel() {
            isClosed = true;
            zipSessions.delete(id);
            try {
                port.close();
            } catch (_) {}
        }
    });

    zipSessions.set(id, {
        stream: stream,
        filename: filename
    });
    port.postMessage({ok: true});
});

self.addEventListener("fetch", function (event) {
    const url = new URL(event.request.url);
    if (!url.pathname.startsWith("/__zip_download__/")) {
        return;
    }

    const id = decodeURIComponent(url.pathname.substring("/__zip_download__/".length));
    const session = zipSessions.get(id);
    if (!session) {
        event.respondWith(new Response("ZIP session not found", {status: 404}));
        return;
    }

    zipSessions.delete(id);
    event.respondWith(new Response(session.stream, {
        headers: {
            "Content-Type": "application/zip",
            "Content-Disposition": contentDisposition(session.filename),
            "Cache-Control": "no-store"
        }
    }));
});
