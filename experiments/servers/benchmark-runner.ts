import { spawn } from 'child_process';
import * as fs from 'fs';

// 6 web servers
const commands = [
    { command: "node-ws", exportName: "A1_NodeServer-HTTP1-WSS" },
    { command: "node-http1", exportName: "A2_NodeServer-HTTP1-SSE" },
    { command: "node-http2", exportName: "A3_NodeServer-HTTP2-SSE" },
    { command: "uws-ws", exportName: "B1_uWebSocketServer-HTTP1-WSS" },
    { command: "uws-http1", exportName: "B2_uWebSocketServer-HTTP1-SSE" },
    { command: "caddy-http3", exportName: "C1_NodeCaddyServer-HTTP3-SSE" }
];

// Time to wait between stopping one service and starting another (in ms)
const COOLDOWN_PERIOD = 2000;

// Time to wait for server to start before running benchmarks (in ms)
const SERVER_STARTUP_WAIT = 6000;

interface ProcessInfo {
    process: ReturnType<typeof spawn>;
    command: string;
}

let currentProcess: ProcessInfo | null = null;

async function cleanupCommand() {
    if (currentProcess) {
        console.log(`Stopping ${currentProcess.command}...`);
        currentProcess.process.kill('SIGKILL');
        await new Promise(resolve => setTimeout(resolve, COOLDOWN_PERIOD));
        currentProcess = null;
    }
}

async function handleShutdown() {
    console.log('\nShutdown signal received. Cleaning up...');
    await cleanupCommand();
    process.exit(0);
}

process.on('SIGINT', handleShutdown);
process.on('SIGTERM', handleShutdown);

async function runCommand(command: string, exportFileName: string, port: number): Promise<void> {
    return new Promise(async (resolve, reject) => {
        try {
            console.log(`\n=== Starting ${command} ===`);
            
            const childProcess = spawn('pnpm', ['run', command], {
                stdio: 'pipe',
                shell: true,
                env: {
                    ...process.env,
                    PORT: `${port}`,
                    LOG_LEVEL: "info",
                    EXPORT_FILENAME: `${exportFileName}.csv`
                }
            });

            currentProcess = {
                process: childProcess,
                command
            };

            childProcess.stdout?.on('data', (data) => {
                console.log(`[${command}] ${data.toString().trim()}`);
            });

            childProcess.stderr?.on('data', (data) => {
                console.error(`[${command}] Error: ${data.toString().trim()}`);
            });

            childProcess.on('exit', (code, signal) => {
                console.log(`[${command}] exited with code: ${code} from signal: ${signal}`);
                resolve();
            });

            await new Promise(resolve => setTimeout(resolve, SERVER_STARTUP_WAIT));
            console.log(`\n=== Running ${command} ===`);

            // Spawn the benchmark process ...
            const outputStream = fs.createWriteStream(`./benchmarks/${exportFileName}.txt`, { flags: 'a' });
            const benchmarkProcess = spawn('pnpm', ['run', 'playwright'], {
            // const benchmarkProcess = spawn('pnpm', ['run', 'load-tests'], {
                stdio: ['ignore', 'pipe', 'pipe'],
                env: {
                    ...process.env,
                    OUTPUT_FILENAME: `./benchmarks/${exportFileName}.json`
                }
            });

            // Pipe the process' output to both the console and an outfile
            benchmarkProcess.stdout?.pipe(outputStream);
            benchmarkProcess.stdout?.on('data', (data) => {
                console.log(`[Benchmark ${command}] ${data.toString().trim()}`);
            });

            benchmarkProcess.stderr?.on('data', (data) => {
                console.error(`[Benchmark Error ${command}] ${data.toString().trim()}`);
            });

            benchmarkProcess.on('error', (err) => {
                console.error(`[Benchmark Error (uncaught)] ${err.toString().trim()}`);
            });

            await new Promise((resolve, reject) => {
                benchmarkProcess.on('exit', (code) => {
                    outputStream.end();
                    if (code === 0) {
                        resolve(code);
                    } else {
                        reject(new Error(`Benchmark failed with code: ${code}`));
                    }
                });
            });

        } catch (error) {
            console.error(`Error running ${command}:`, error);
            await cleanupCommand();
            reject(error);
        }
    });
}

async function runBenchmarks() {
    if (!fs.existsSync('./benchmarks')) {
        fs.mkdirSync('./benchmarks', { recursive: true });
    }

    try {
        for (const command of commands) {
            await runCommand(command.command, command.exportName, 3000);
            await cleanupCommand();
        }
        console.log('\n=== All benchmarks completed ===');
    } catch (error) {
        console.error('Benchmark suite failed:', error);
        process.exit(1);
    }
}

runBenchmarks();