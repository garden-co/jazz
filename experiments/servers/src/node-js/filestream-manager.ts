import fs from "fs";
import { Response } from "express";
import { RawReplyDefaultExpression } from "fastify"
import {
    CoValue,
    WebSocketResponse,
    File,
    covalues,
    addCoValue,
    CHUNK_SIZE,
    uWebSocketResponse,
    FastifyResponseWrapper
} from "../util";
import logger from "../util/logger";

export interface UploadBody {
    uuid: string;
    filename: string;
    chunk: string;
    chunks: string;
    base64: string;
}

export interface UploadState {
    targetPath: string;
    receivedChunks: Set<number>;
    totalChunks: number;
    originalFilename: string;
}

interface StreamOptions {
    uuid: string;
    filePath: string;
    range?: string;
    fileName?: string;
    headers?: Record<string, string>;
}

interface StreamTarget {
    type: "http" | "websocket";
    res?: Response | RawReplyDefaultExpression;
    wsr?: WebSocketResponse | uWebSocketResponse;
}

export class FileStreamManager {
    private uploads: Map<string, UploadState>;

    constructor() {
        this.uploads = new Map<string, UploadState>();
    }

    // upload methods
    async chunkFileUpload(
        payload: UploadBody,
        res: WebSocketResponse | uWebSocketResponse | Response | FastifyResponseWrapper,
    ) {
        const { uuid, filename, base64, chunk, chunks } = payload;
        const chunkIndex = parseInt(chunk, 10);
        const totalChunks = parseInt(chunks, 10);

        if (
            !uuid ||
            !filename ||
            !base64 ||
            chunkIndex == null ||
            !totalChunks
        ) {
            return res.status(400).json({
                m: `Missing required fields. uuid: ${uuid}, filename: ${filename}, chunk: ${chunkIndex}, chunks: ${totalChunks}.`,
            });
        }

        if (!this.uploads.has(uuid)) {
            const ext = filename.substring(
                filename.lastIndexOf("."),
                filename.length,
            );
            const targetPath = `public/uploads/${uuid}${ext}`;

            this.uploads.set(uuid, {
                targetPath,
                receivedChunks: new Set(),
                totalChunks,
                originalFilename: filename,
            });

            fs.writeFileSync(targetPath, "");
        }

        const uploadState = this.uploads.get(uuid)!;

        try {
            // Check if we already received this chunk
            if (uploadState.receivedChunks.has(chunkIndex)) {
                res.status(202).json({
                    m: `Chunk ${chunkIndex} already received`,
                });
                return;
            }

            // Write chunk to file at the correct position
            const writeStream = fs.createWriteStream(uploadState.targetPath, {
                flags: "r+",
                start: chunkIndex * CHUNK_SIZE,
            });

            const buffer = Buffer.from(base64, "base64");
            writeStream.write(buffer);
            writeStream.end();

            // Mark chunk as received
            uploadState.receivedChunks.add(chunkIndex);

            // Check if upload is complete
            logger.debug(
                `[POST] Received chunk ${uploadState.receivedChunks.size} of ${totalChunks} with size ${buffer.length}`,
            );
            if (uploadState.receivedChunks.size === totalChunks) {
                await this.handleCompletedUpload(uuid, uploadState);
                res.status(201).json({ m: "OK" });
            } else {
                res.status(200).json({ m: "OK" });
            }
        } catch (err: unknown) {
            const msg = `[POST] Error processing chunk ${chunkIndex} for file '${uploadState.originalFilename}'`;
            if (err instanceof Error) logger.error(err.stack);
            logger.error(msg, err);
            res.status(500).json({ m: msg });
        }
    }

    async handleCompletedUpload(uuid: string, uploadState: UploadState) {
        const file: File = {
            name: uploadState.originalFilename,
            path: uploadState.targetPath,
        };

        const covalue: CoValue = {
            uuid,
            lastUpdated: new Date(),
            author: "",
            title: "",
            summary: "",
            preview: "",
            url: file,
        };

        addCoValue(covalues, covalue);
        this.uploads.delete(uuid);

        logger.debug(
            `[POST] Chunked upload of ${uploadState.totalChunks} chunks for file '${uploadState.originalFilename}' completed successfully.`,
        );
    }

    // download methods
    validateFilePath(filePath: string): {
        valid: boolean;
        fileSize?: number;
        error?: string;
    } {
        try {
            if (!filePath) {
                return { valid: false, error: "File path is required" };
            }
            const stat = fs.statSync(filePath);
            return { valid: true, fileSize: stat.size };
        } catch (error) {
            return { valid: false, error: "File not found or inaccessible" };
        }
    }

    calculateRange(range: string | undefined, fileSize: number) {
        let start = 0;
        let end = fileSize - 1;

        if (range) {
            const parts = range.replace(/bytes=/, "").split("-");
            start = parseInt(parts[0], 10);
            end = Math.min(start + CHUNK_SIZE, fileSize - 1);
        }

        return { start, end, contentLength: end - start + 1 };
    }

    chunkFileDownloadError(
        error: Error,
        target: StreamTarget,
        fileStream: fs.ReadStream,
    ) {
        logger.error("[GET] Error in file stream: ", error);
        fileStream.destroy();

        if (target.type === "websocket" && target.wsr) {
            target.wsr.status(500).json({ m: "Error reading file" });

        } else if (target.type === "http" && target.res) {
            const errorChunk = JSON.stringify({
                type: "error",
                message: `Error occurred while streaming the file: ${error.message}`
            }) + '\n';

            target.res.end(errorChunk);
            // target.res.status(500).json({ m: "Error reading file" }); // .json() will set headers after they have already been sent
        }
    }

    async chunkFileDownload(
        options: StreamOptions,
        target: StreamTarget,
    ): Promise<void> {
        const { uuid, filePath, range, fileName = "sample.zip", headers } = options;

        const validation = this.validateFilePath(filePath);
        if (!validation.valid) {
            if (target.type === "websocket" && target.wsr) {
                target.wsr.status(404).json({ m: validation.error });
            } else if (target.type === "http" && target.res) {
                if (target.res instanceof Response) {
                    (target.res as Response).status(404).json({ m: validation.error });
                } else {
                    const r = (target.res as RawReplyDefaultExpression);
                    r.statusCode = 404;
                    r.setHeader('Content-Type', 'application/json');
                    r.end(JSON.stringify({ m: validation.error }));
                }
            }
            return;
        }

        const fileSize = validation.fileSize!;
        const { start, end, contentLength } = this.calculateRange(
            range,
            fileSize,
        );

        if (target.type === "websocket" && target.wsr) {
            target.wsr.status(202).json({
                contentType: "application/json",
                fileName,
                // contentLength,
                fileSize,
                start,
                end,
            });
        } else if (target.type === "http" && target.res) {
            headers!["Content-Range"] = `bytes ${start}-${end}/${fileSize}`;
            target.res.writeHead(206, headers);
        }

        logger.debug(
            `[GET] Streaming file '${filePath}' of size ${fileSize} (${
                fileSize / 1_000_000
            }MB) in ${CHUNK_SIZE / 1024}KB chunks...`,
        );

        const fileStream = fs.createReadStream(filePath, {
            highWaterMark: CHUNK_SIZE,
        });

        const totalChunks = Math.ceil(fileSize / CHUNK_SIZE);
        let chunkIndex = 0;
        let streamEnded = false;

        const metadataChunk = JSON.stringify({
            type: "metadata",
            uuid,
            fileName,
            fileSize,
            chunkSize: CHUNK_SIZE,
            totalChunks: totalChunks
        }) + '\n';

        if (target.type === "websocket" && target.wsr) {
            // TODO:

        } else if (target.type === "http" && target.res) {
            target.res.write(metadataChunk, (error) => {
                if (error) {
                    this.chunkFileDownloadError(error, target, fileStream);
                    return;
                }
            });
        }

        fileStream.on('data', (chunk) => {
            fileStream.pause();

            const base64Data = chunk.toString('base64');
            const chunkData = JSON.stringify({
              type: "chunk",
              uuid,
              chunkIndex: chunkIndex++,
              data: base64Data
            }) + '\n';


            if (target.type === "websocket" && target.wsr) {
                // TODO:

            } else if (target.type === "http" && target.res) {
                target.res.write(chunkData, (error) => {
                    if (error) {
                        this.chunkFileDownloadError(error, target, fileStream);
                        return;
                    } else {
                        if (!streamEnded) {
                            fileStream.resume();
                        }
                    }
                });
            }
        });


        fileStream.on("end", () => {
            streamEnded = true;
            if (target.type === "websocket" && target.wsr) {
                target.wsr.status(204).json({ m: "OK" });
                // TODO:

            } else if (target.type === "http" && target.res) {
                const finalChunk = JSON.stringify({
                  type: "end",
                  chunkIndex,
                  totalChunks
                }) + '\n';

                target.res.end(finalChunk);
            }
            logger.debug(
                `[GET] Chunked download of file '${filePath}' with size ${fileSize} (${
                    fileSize / 1_000_000
                }MB) completed successfully.`,
            );
        });

        fileStream.on("error", (error) => {
            this.chunkFileDownloadError(error, target, fileStream);
        });
    }
}
