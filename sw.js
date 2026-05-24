const jobs = new Map();
const encoder = new TextEncoder();
const crc_table = make_crc_table();
let activeZipDownloads = 0;

self.addEventListener("install", function (event) {
    event.waitUntil(self.skipWaiting());
});

self.addEventListener("message", function (event) {

    if (event.data.type === "ZIP_KEEPALIVE_PING") {
        event.ports[0].postMessage({zipDownloadActive: jobs.size > 0 || activeZipDownloads > 0});
    }
    else if (event.data.type === "ZIP_CREATE_JOB") {
        jobs.set(event.data.token, {
            filename: event.data.filename,
            files: event.data.files
        });
        event.ports[0].postMessage({ok: true});
    }
    else if (event.data.type === "CLAIM_CLIENTS") {
        event.waitUntil(self.clients.claim());
    }
});

self.addEventListener("fetch", function (event) {
    const url = new URL(event.request.url);
    if (event.request.method !== "GET" || url.origin !== self.location.origin) {
        return;
    }

    if (!url.pathname.startsWith("/__zip_download__/")) {
        return;
    }

    event.respondWith(handle_zip_download(url));
});

function handle_zip_download(url) {
    const token = decodeURIComponent(url.pathname.substring("/__zip_download__/".length).split("/")[0]);
    const job = jobs.get(token);
    if (!job) {
        return new Response("ZIP job was not found. Try starting the download again.", {
            status: 404,
            headers: {"Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-store"}
        });
    }

    jobs.delete(token);
    const zipResponse = stream_zip_response(job);
    activeZipDownloads++;
    zipResponse.done.then(function () {
        activeZipDownloads--;
    });
    return zipResponse.response;
}

function stream_zip_response(job) {
    const files = job.files;
    const contentLength = calculate_zip_content_length(files);
    const headers = {
        "Content-Type": "application/zip",
        "Content-Disposition": content_disposition(job.filename),
        "Cache-Control": "no-store",
        "Content-Length": contentLength.toString()
    };

    const iterator = generate_zip_chunks(files);
    let finish_stream;
    const done = new Promise(function (resolve) {
        finish_stream = resolve;
    });
    let streamCancelled = false;

    const stream = new ReadableStream({
        async pull(controller) {
            try {
                const read = await iterator.next();
                if (streamCancelled) {
                    return;
                }

                if (read.done) {
                    controller.close();
                    finish_stream();
                    return;
                }

                controller.enqueue(read.value);
            } catch (err) {
                if (streamCancelled) {
                    return;
                }

                console.error(err);
                controller.error(err);
                finish_stream();
            }
        },
        cancel() {
            streamCancelled = true;
            iterator.return().catch(function () {});
            finish_stream();
        }
    });

    return {
        response: new Response(stream, {
            headers: headers
        }),
        done: done
    };
}

async function* generate_zip_chunks(files) {
    let offset = 0n;
    const centralDirectory = [];

    function chunk(value) {
        offset += BigInt(value.byteLength);
        return value;
    }

    for (const file of files) {
        const zipPath = clean_zip_path(file.path);
        if (!zipPath) {
            continue;
        }

        const nameBytes = encoder.encode(zipPath);
        const dosTimeDate = make_dos_time_date(file.mtime);
        const localHeaderOffset = offset;
        yield chunk(make_local_file_header(nameBytes, dosTimeDate));

        const response = await fetch(file.url, {credentials: "same-origin"});
        if (!response.ok || !response.body) {
            throw new Error("Failed to fetch " + zipPath + ": " + response.status + " " + response.statusText);
        }

        const reader = response.body.getReader();
        let crc = 0xffffffff;
        let size = 0n;

        while (true) {
            const read = await reader.read();
            if (read.done) {
                break;
            }

            crc = crc32_update(crc, read.value);
            size += BigInt(read.value.byteLength);
            yield chunk(read.value);
        }

        const crc32 = crc32_finish(crc);
        yield chunk(make_zip64_data_descriptor(crc32, size, size));

        centralDirectory.push({
            nameBytes: nameBytes,
            dosTimeDate: dosTimeDate,
            crc32: crc32,
            compressedSize: size,
            uncompressedSize: size,
            localHeaderOffset: localHeaderOffset
        });
    }

    const centralDirectoryOffset = offset;
    for (const entry of centralDirectory) {
        yield chunk(make_central_directory_header(entry));
    }
    const centralDirectorySize = offset - centralDirectoryOffset;
    const zip64EndOffset = offset;

    yield chunk(make_zip64_end_of_central_directory(centralDirectory.length, centralDirectorySize, centralDirectoryOffset));
    yield chunk(make_zip64_end_of_central_directory_locator(zip64EndOffset));
    yield chunk(make_end_of_central_directory());
}

function calculate_zip_content_length(files) {
    let total = 0n;
    let centralDirectoryLength = 0n;

    for (const file of files) {
        const zipPath = clean_zip_path(file.path);
        if (!zipPath) {
            continue;
        }

        const size = BigInt(file.size);

        const nameBytes = encoder.encode(zipPath);
        total += BigInt(30 + nameBytes.length); // local file header
        total += size;
        total += 24n; // ZIP64 data descriptor: signature + crc32 + two uint64 sizes
        centralDirectoryLength += BigInt(46 + 28 + nameBytes.length); // central header + ZIP64 extra with three uint64 values
    }

    total += centralDirectoryLength;
    total += 56n + 20n + 22n; // ZIP64 end record + ZIP64 locator + classic end record
    return total;
}

function make_local_file_header(nameBytes, dosTimeDate) {
    const writer = new BinaryWriter(30 + nameBytes.length); // local file header + filename
    writer.u32(0x04034b50);
    writer.u16(45); // version needed to extract: ZIP64 / 4.5
    writer.u16(0x0808); // data descriptor follows, filename is UTF-8
    writer.u16(0);
    writer.u16(dosTimeDate.time);
    writer.u16(dosTimeDate.date);
    writer.u32(0);
    writer.u32(0);
    writer.u32(0);
    writer.u16(nameBytes.length);
    writer.u16(0);
    writer.bytes(nameBytes);
    return writer.finish();
}

function make_zip64_data_descriptor(crc32, compressedSize, uncompressedSize) {
    const writer = new BinaryWriter(24); // signature + crc32 + two uint64 sizes
    writer.u32(0x08074b50);
    writer.u32(crc32);
    writer.u64(compressedSize);
    writer.u64(uncompressedSize);
    return writer.finish();
}

function make_central_directory_header(entry) {
    const extra = make_zip64_extra([entry.uncompressedSize, entry.compressedSize, entry.localHeaderOffset]);
    const writer = new BinaryWriter(46 + entry.nameBytes.length + extra.length); // central file header + filename + extra fields
    writer.u32(0x02014b50);
    writer.u16(45); // version made by: ZIP64 / 4.5
    writer.u16(45); // version needed to extract: ZIP64 / 4.5
    writer.u16(0x0808); // data descriptor follows, filename is UTF-8
    writer.u16(0);
    writer.u16(entry.dosTimeDate.time);
    writer.u16(entry.dosTimeDate.date);
    writer.u32(entry.crc32);
    writer.u32(0xffffffff);
    writer.u32(0xffffffff);
    writer.u16(entry.nameBytes.length);
    writer.u16(extra.length);
    writer.u16(0);
    writer.u16(0);
    writer.u16(0);
    writer.u32(0);
    writer.u32(0xffffffff);
    writer.bytes(entry.nameBytes);
    writer.bytes(extra);
    return writer.finish();
}

function make_zip64_end_of_central_directory(entryCount, centralDirectorySize, centralDirectoryOffset) {
    const writer = new BinaryWriter(56); // ZIP64 end of central directory record
    writer.u32(0x06064b50);
    writer.u64(44n); // remaining ZIP64 end record size after signature and this size field
    writer.u16(45); // version made by: ZIP64 / 4.5
    writer.u16(45); // version needed to extract: ZIP64 / 4.5
    writer.u32(0);
    writer.u32(0);
    writer.u64(BigInt(entryCount));
    writer.u64(BigInt(entryCount));
    writer.u64(centralDirectorySize);
    writer.u64(centralDirectoryOffset);
    return writer.finish();
}

function make_zip64_end_of_central_directory_locator(zip64EndOffset) {
    const writer = new BinaryWriter(20); // ZIP64 end of central directory locator
    writer.u32(0x07064b50);
    writer.u32(0);
    writer.u64(zip64EndOffset);
    writer.u32(1);
    return writer.finish();
}

function make_end_of_central_directory() {
    const writer = new BinaryWriter(22); // classic end of central directory record
    writer.u32(0x06054b50);
    writer.u16(0);
    writer.u16(0);
    writer.u16(0xffff);
    writer.u16(0xffff);
    writer.u32(0xffffffff);
    writer.u32(0xffffffff);
    writer.u16(0);
    return writer.finish();
}

function make_zip64_extra(values) {
    const writer = new BinaryWriter(4 + values.length * 8);
    writer.u16(0x0001);
    writer.u16(values.length * 8);
    for (const value of values) {
        writer.u64(value);
    }
    return writer.finish();
}

class BinaryWriter {
    constructor(length) {
        this.buffer = new ArrayBuffer(length);
        this.view = new DataView(this.buffer);
        this.offset = 0;
    }

    u16(value) {
        this.view.setUint16(this.offset, value, true);
        this.offset += 2;
    }

    u32(value) {
        this.view.setUint32(this.offset, value >>> 0, true);
        this.offset += 4;
    }

    u64(value) {
        let n = BigInt(value);
        this.view.setUint32(this.offset, Number(n & 0xffffffffn), true);
        this.view.setUint32(this.offset + 4, Number((n >> 32n) & 0xffffffffn), true);
        this.offset += 8;
    }

    bytes(value) {
        new Uint8Array(this.buffer, this.offset, value.length).set(value);
        this.offset += value.length;
    }

    finish() {
        return new Uint8Array(this.buffer);
    }
}

function clean_zip_path(path) {
    return path.replace(/\\/g, "/").split("/").filter(function (part) {
        return part.length > 0 && part !== "." && part !== "..";
    }).join("/");
}

function make_dos_time_date(value) {
    const date = new Date(value);
    if (!value || isNaN(date.getTime()) || date.getFullYear() < 1980) {
        return {time: 0, date: 33}; // DOS date 1980-01-01
    }

    const year = Math.min(date.getFullYear(), 2107);
    return {
        time: (date.getHours() << 11) | (date.getMinutes() << 5) | Math.floor(date.getSeconds() / 2),
        date: ((year - 1980) << 9) | ((date.getMonth() + 1) << 5) | date.getDate()
    };
}

function make_crc_table() {
    const table = new Uint32Array(256);
    for (let i = 0; i < 256; i++) {
        let c = i;
        for (let j = 0; j < 8; j++) {
            c = (c & 1) ? (0xedb88320 ^ (c >>> 1)) : (c >>> 1);
        }
        table[i] = c >>> 0;
    }
    return table;
}

function crc32_update(crc, bytes) {
    for (let i = 0; i < bytes.length; i++) {
        crc = crc_table[(crc ^ bytes[i]) & 0xff] ^ (crc >>> 8);
    }
    return crc;
}

function crc32_finish(crc) {
    return (crc ^ 0xffffffff) >>> 0;
}

function content_disposition(filename) {
    const fallback = filename.replace(/["\\\r\n]/g, "_").replace(/[^\x20-\x7e]/g, "_");
    return "attachment; filename=\"" + fallback + "\"; filename*=UTF-8''" + encode_rfc5987(filename);
}

function encode_rfc5987(value) {
    return encodeURIComponent(value).replace(/['()]/g, function (char) {
        return "%" + char.charCodeAt(0).toString(16).toUpperCase();
    }).replace(/\*/g, "%2A");
}

