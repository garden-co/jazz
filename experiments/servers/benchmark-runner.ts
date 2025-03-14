import { spawn } from 'child_process';
import * as fs from 'fs';
import { setNetworkCondition, resetNetwork } from './src/util/network-conditioner';

// Network conditions I - IV
const networkConditions = [
    { name: 'ideal-network', prefix: "I" },    // Ideal network, no bandwidth limits or latency
    { name: '4g-speeds', prefix: "II" },       // 4G simulation
    { name: '3g-speeds', prefix: "III" },       // 3G simulation
    { name: 'high-packet-loss', prefix: "IV" }, // High packet loss
];

// 6 web servers
const commands = [
    { command: "node-ws", exportName: "A1_NodeServer-HTTP1-WSS" },
    { command: "node-http1", exportName: "A2_NodeServer-HTTP1-SSE" },
    { command: "node-http2", exportName: "A3_NodeServer-HTTP2-SSE" },
    { command: "uws-ws", exportName: "B1_uWebSocketServer-HTTP1-WSS" },
    { command: "uws-http1", exportName: "B2_uWebSocketServer-HTTP1-SSE" },
    { command: "caddy-http3", exportName: "C1_NodeCaddyServer-HTTP3-SSE" }
];

// Time to wait between stopping one web server and starting another (in ms)
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
    console.log('\nRestoring normal network conditions ...');
    await resetNetwork();
    process.exit(0);
}

process.on('SIGINT', handleShutdown);
process.on('SIGTERM', handleShutdown);

async function runCommand(command: string, exportFileName: string, port: number): Promise<void> {
    return new Promise(async (resolve, reject) => {
        try {
            console.log(`\n=== Starting ${command} ===`);
            let terminated = false;
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
                terminated = true;
                console.log(`[${command}] exited with code: ${code} from signal: ${signal}`);
                resolve();
            });

            await new Promise(resolve => setTimeout(resolve, SERVER_STARTUP_WAIT));
            console.log(`\n=== Running ${command} ===`);

            // Spawn the benchmark tests ...
            const outputStream = fs.createWriteStream(`${exportFileName}.txt`, { flags: 'a' });
            // const benchmarkProcess = spawn('pnpm', ['run', 'playwright'], {
            const benchmarkProcess = spawn('pnpm', ['run', 'load-tests-demo'], {
                stdio: ['ignore', 'pipe', 'pipe'],
                env: {
                    ...process.env,
                    PID: `${childProcess.pid}`,
                    OUTPUT_FILENAME: `${exportFileName}.json`
                }
            });

            // Pipe the processes' output to both the console and an outfile
            benchmarkProcess.stdout?.pipe(outputStream);
            benchmarkProcess.stdout?.on('data', (data) => {
                console.log(`[Benchmark ${command}] ${data.toString().trim()}`);
            });

            benchmarkProcess.stderr?.on('data', (data) => {
                console.error(`[Benchmark Error ${command}] ${data.toString().trim()}`);
            });

            await new Promise((resolve, reject) => {
                let hasResolved = false;

                // Function to ensure we only resolve/reject once
                const finalizeProcess = (code: number) => {
                    if (hasResolved) return;
                    hasResolved = true;

                    console.log(`Benchmark test ended with code: ${code}`);
                    outputStream.end();
                    console.log(`Benchmark test output file closed: ${exportFileName}.txt`);

                    if (code === 0) {
                        console.log(`Benchmark test succeeded`);
                        resolve(code);
                    } else {
                        console.log(`Benchmark test failed`);
                        reject(new Error(`Benchmark test failed with code: ${code}`));
                    }
                };

                benchmarkProcess.on('close', finalizeProcess); // artillery process fires `close`
                benchmarkProcess.on('exit', finalizeProcess); // playwright process fires `exit`
                benchmarkProcess.on('error', (err) => {
                    if (hasResolved) return;
                    hasResolved = true;
                    outputStream.end();
                    reject(err);
                });
            });

            if (!terminated) {
                console.log(`[${command}] terminating forcefully`);
                childProcess.kill();
                resolve();
            }
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

    let exitCode = 0;
    try {

        for (const condition of networkConditions) {
            console.log(`Applying network condition with short name: '${condition.name}'`);
            await setNetworkCondition(condition.name);

            for (const command of commands) {
                const folder = `./benchmarks/${condition.prefix}-${condition.name}`;
                fs.mkdirSync(folder, { recursive: true });
                await runCommand(command.command, `${folder}/${command.exportName}`, 3000);
                await cleanupCommand();
            }
            console.log(`\n=== All benchmarks completed under '${condition.name}' network condition. ===`);
        }
    } catch (error) {
        console.error('Benchmark suite failed:', error);
        exitCode = 1;
    } finally {
        // Restore the network conditions even when tests fail
        console.log("Terminating the benchmarks ....");
        await resetNetwork();
        process.exit(exitCode);
    }
}

runBenchmarks();
