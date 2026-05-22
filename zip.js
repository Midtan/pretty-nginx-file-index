function zipSupportsServiceWorkerStreamingDownload() {
    return typeof navigator !== "undefined"
        && "serviceWorker" in navigator
        && typeof ReadableStream === "function"
        && typeof MessageChannel === "function";
}

function zipSupportsBrowserDownload() {
    return zipSupportsServiceWorkerStreamingDownload();
}

function dosDateTime(jsDate) {
    let date = 0;
    let time = 0;
    const year = jsDate.getFullYear();
    if (year < 1980) {
        return {date: (1 << 5) | 1, time: 0};
    }
    date = ((year - 1980) << 9) | ((jsDate.getMonth() + 1) << 5) | jsDate.getDate();
    time = (jsDate.getHours() << 11) | (jsDate.getMinutes() << 5) | Math.floor(jsDate.getSeconds() / 2);
    return {date, time};
}

function crcTable() {
    return crcTable.table || (crcTable.table = (function () {
        const table = [];
        for (let n = 0; n < 256; n++) {
            let c = n;
            for (let k = 0; k < 8; k++) {
                c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
            }
            table[n] = c >>> 0;
        }
        return table;
    })());
}

function crc32Update(current, chunk) {
    const table = crcTable();
    let crc = current;
    for (let i = 0; i < chunk.length; i++) {
        crc = (crc >>> 8) ^ table[(crc ^ chunk[i]) & 0xFF];
    }
    return crc >>> 0;
}

function dvBytes(size, fill) {
    const dv = new DataView(new ArrayBuffer(size));
    fill(dv);
    return new Uint8Array(dv.buffer);
}

function uint64LEBytes(value) {
    let v = BigInt(value);
    const out = new Uint8Array(8);
    for (let i = 0; i < 8; i++) {
        out[i] = Number(v & 0xFFn);
        v >>= 8n;
    }
    return out;
}

async function writeZipToSink(fileEntries, sink, onProgress) {
    const progressCallback = onProgress || function () {};

    let archiveOffset = 0n;
    const centralDirectoryEntries = [];
    let writtenFiles = 0;

    try {
        for (let i = 0; i < fileEntries.length; i++) {
            const entry = fileEntries[i];
            const normalizedPath = entry.path.replace(/^\/+/, "").replace(/\\/g, "/");
            const nameBytes = new TextEncoder().encode(normalizedPath);
            const {date, time} = dosDateTime(entry.date || new Date());
            const localHeaderOffset = archiveOffset;

            const localHeader = dvBytes(30, function (dv) {
                dv.setUint32(0, 0x04034b50, true);
                dv.setUint16(4, 45, true);
                dv.setUint16(6, 0x0008, true); // data descriptor follows file data
                dv.setUint16(8, 0, true); // store only
                dv.setUint16(10, time, true);
                dv.setUint16(12, date, true);
                dv.setUint32(14, 0, true);
                dv.setUint32(18, 0, true);
                dv.setUint32(22, 0, true);
                dv.setUint16(26, nameBytes.length, true);
                dv.setUint16(28, 0, true);
            });

            await sink.write(localHeader);
            await sink.write(nameBytes);
            archiveOffset += BigInt(localHeader.length + nameBytes.length);

            const response = await fetch(entry.url);
            if (!response.ok) {
                throw new Error("Failed to fetch " + entry.url + ": " + response.status + " " + response.statusText);
            }

            let crc = 0xFFFFFFFF;
            let size = 0n;

            if (response.body && response.body.getReader) {
                const reader = response.body.getReader();
                while (true) {
                    const read = await reader.read();
                    if (read.done) {
                        break;
                    }
                    const chunk = read.value;
                    size += BigInt(chunk.length);
                    crc = crc32Update(crc, chunk);
                    await sink.write(chunk);
                }
            } else {
                const chunk = new Uint8Array(await response.arrayBuffer());
                size = BigInt(chunk.length);
                crc = crc32Update(crc, chunk);
                await sink.write(chunk);
            }

            crc = (crc ^ 0xFFFFFFFF) >>> 0;
            archiveOffset += size;
            const dataDescriptor = dvBytes(24, function (dv) {
                dv.setUint32(0, 0x08074b50, true);
                dv.setUint32(4, crc, true);
            });
            dataDescriptor.set(uint64LEBytes(size), 8);
            dataDescriptor.set(uint64LEBytes(size), 16);
            await sink.write(dataDescriptor);
            archiveOffset += BigInt(dataDescriptor.length);

            const zip64CentralExtra = new Uint8Array(28);
            const zip64CentralExtraDV = new DataView(zip64CentralExtra.buffer);
            zip64CentralExtraDV.setUint16(0, 0x0001, true);
            zip64CentralExtraDV.setUint16(2, 24, true);
            zip64CentralExtra.set(uint64LEBytes(size), 4);
            zip64CentralExtra.set(uint64LEBytes(size), 12);
            zip64CentralExtra.set(uint64LEBytes(localHeaderOffset), 20);

            centralDirectoryEntries.push({
                pathBytes: nameBytes,
                zip64Extra: zip64CentralExtra,
                crc32: crc,
                date: date,
                time: time
            });

            writtenFiles++;
            progressCallback({
                totalFiles: fileEntries.length,
                completedFiles: writtenFiles,
                currentPath: normalizedPath
            });
        }

        const centralDirectoryOffset = archiveOffset;
        for (let i = 0; i < centralDirectoryEntries.length; i++) {
            const cde = centralDirectoryEntries[i];
            const cdHeader = dvBytes(46, function (dv) {
                dv.setUint32(0, 0x02014b50, true);
                dv.setUint16(4, 45, true);
                dv.setUint16(6, 45, true);
                dv.setUint16(8, 0x0008, true);
                dv.setUint16(10, 0, true);
                dv.setUint16(12, cde.time, true);
                dv.setUint16(14, cde.date, true);
                dv.setUint32(16, cde.crc32, true);
                dv.setUint32(20, 0xFFFFFFFF, true);
                dv.setUint32(24, 0xFFFFFFFF, true);
                dv.setUint16(28, cde.pathBytes.length, true);
                dv.setUint16(30, cde.zip64Extra.length, true);
                dv.setUint16(32, 0, true);
                dv.setUint16(34, 0, true);
                dv.setUint16(36, 0, true);
                dv.setUint32(38, 0, true);
                dv.setUint32(42, 0xFFFFFFFF, true);
            });

            await sink.write(cdHeader);
            await sink.write(cde.pathBytes);
            await sink.write(cde.zip64Extra);
            archiveOffset += BigInt(cdHeader.length + cde.pathBytes.length + cde.zip64Extra.length);
        }

        const centralDirectorySize = archiveOffset - centralDirectoryOffset;
        const zip64EndOffset = archiveOffset;
        const zip64EndRecord = dvBytes(56, function (dv) {
            dv.setUint32(0, 0x06064b50, true);
            dv.setUint32(4, 44, true);
            dv.setUint32(8, 0, true);
            dv.setUint16(12, 45, true);
            dv.setUint16(14, 45, true);
            dv.setUint32(16, 0, true);
            dv.setUint32(20, 0, true);
        });
        zip64EndRecord.set(uint64LEBytes(centralDirectoryEntries.length), 24);
        zip64EndRecord.set(uint64LEBytes(centralDirectoryEntries.length), 32);
        zip64EndRecord.set(uint64LEBytes(centralDirectorySize), 40);
        zip64EndRecord.set(uint64LEBytes(centralDirectoryOffset), 48);
        await sink.write(zip64EndRecord);
        archiveOffset += BigInt(zip64EndRecord.length);

        const zip64Locator = dvBytes(20, function (dv) {
            dv.setUint32(0, 0x07064b50, true);
            dv.setUint32(4, 0, true);
            dv.setUint32(16, 1, true);
        });
        zip64Locator.set(uint64LEBytes(zip64EndOffset), 8);
        await sink.write(zip64Locator);
        archiveOffset += BigInt(zip64Locator.length);

        const endRecord = dvBytes(22, function (dv) {
            dv.setUint32(0, 0x06054b50, true);
            dv.setUint16(4, 0, true);
            dv.setUint16(6, 0, true);
            dv.setUint16(8, 0xFFFF, true);
            dv.setUint16(10, 0xFFFF, true);
            dv.setUint32(12, 0xFFFFFFFF, true);
            dv.setUint32(16, 0xFFFFFFFF, true);
            dv.setUint16(20, 0, true);
        });
        await sink.write(endRecord);
        await sink.close();
    } catch (err) {
        await sink.abort();
        throw err;
    }
}

let zipSwReadyPromise = null;

async function ensureZipDownloadServiceWorker() {
    if (zipSwReadyPromise) {
        return zipSwReadyPromise;
    }
    zipSwReadyPromise = (async function () {
        const reg = await navigator.serviceWorker.register("/zip.worker.js", {scope: "/"});
        await navigator.serviceWorker.ready;
        return reg;
    })();
    return zipSwReadyPromise;
}

function postMessageAwaitAck(target, message, transferables) {
    return new Promise(function (resolve, reject) {
        const channel = new MessageChannel();
        channel.port1.onmessage = function (event) {
            const data = event.data || {};
            if (data.ok) {
                resolve(channel.port1);
                return;
            }
            reject(new Error(data.error || "Service Worker initialization failed."));
        };
        target.postMessage(message, (transferables || []).concat(channel.port2));
    });
}

function triggerServiceWorkerDownload(downloadUrl) {
    const anchor = document.createElement("a");
    anchor.href = downloadUrl;
    anchor.rel = "noopener";
    anchor.style.display = "none";
    document.body.appendChild(anchor);
    anchor.click();
    document.body.removeChild(anchor);
}

async function createZipStreamViaServiceWorkerDownload(fileEntries, options) {
    const opts = options || {};
    const fileName = opts.fileName || "archive.zip";
    const onProgress = opts.onProgress || function () {};

    if (!zipSupportsServiceWorkerStreamingDownload()) {
        throw new Error("Service Worker streaming download is not supported in this browser.");
    }
    if (!window.isSecureContext) {
        throw new Error("Service Worker download requires HTTPS or localhost.");
    }

    const reg = await ensureZipDownloadServiceWorker();
    if (!navigator.serviceWorker.controller) {
        throw new Error("Service Worker is installed but not controlling this page yet. Reload once and try again.");
    }
    const swTarget = navigator.serviceWorker.controller || reg.active || reg.waiting || reg.installing;
    if (!swTarget) {
        throw new Error("Service Worker is not active.");
    }

    const downloadId = "zip-" + Date.now() + "-" + Math.random().toString(36).slice(2);
    const filename = fileName.replace(/[\r\n"]/g, "_");
    const port = await postMessageAwaitAck(swTarget, {
        type: "zip-download-init",
        id: downloadId,
        filename: filename
    }, []);

    const sink = {
        async write(chunk) {
            const out = chunk.buffer.slice(chunk.byteOffset, chunk.byteOffset + chunk.byteLength);
            port.postMessage({type: "chunk", data: out}, [out]);
        },
        async close() {
            port.postMessage({type: "end"});
            port.close();
        },
        async abort() {
            port.postMessage({type: "abort"});
            port.close();
        }
    };

    const downloadUrl = "/__zip_download__/" + encodeURIComponent(downloadId);
    triggerServiceWorkerDownload(downloadUrl);
    await writeZipToSink(fileEntries, sink, onProgress);
}

async function createZipDownload(fileEntries, options) {
    return createZipStreamViaServiceWorkerDownload(fileEntries, options);
}
