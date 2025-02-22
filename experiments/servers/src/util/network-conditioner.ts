import { promisify } from 'util';
const exec = promisify(require('child_process').exec);

// Interface for network condition configuration
interface NetworkCondition {
  name: string;
  downBw?: string; // Bandwidth in "Mbit/s" or undefined for no limit
  upBw?: string;
  delay?: string; // Delay in ms or "0" for no added latency
  plr?: string; // Packet loss rate (0.0 to 1.0)
}

// Function to reset network conditions to default
async function resetNetwork(): Promise<void> {
  console.log('Resetting network conditions...');
  try {
    await exec('sudo pfctl -f /etc/pf.conf 2>/dev/null');
    
    // await exec('sudo pfctl -d 2>/dev/null');
    try {
        await exec('sudo pfctl -d');
      } catch (error: any) {
        if (error.code === 1 && error.stderr.includes('pf not enabled')) {
          console.log('pf was already disabled, continuing...');
        } else {
          throw error;
        }
    }

    await exec('sudo dnctl -q flush 2>/dev/null');
    console.log('Network reset to default (no limits).');
  } catch (error) {
    console.error('Error resetting network:', error);
    throw error;
  }
}

// Function to apply a network condition
async function applyCondition(condition: NetworkCondition): Promise<void> {
  console.log(`Applying network condition: ${condition.name}`);

  try {
    // Flush existing dummynet pipes
    await exec('sudo dnctl -q flush');

    if (condition.downBw && condition.upBw && condition.delay && condition.plr) {
      // Configure pipes: pipe 1 for outbound, pipe 2 for inbound
      await exec(`sudo dnctl pipe 1 config bw ${condition.downBw} delay ${condition.delay} plr ${condition.plr}`);
      await exec(`sudo dnctl pipe 2 config bw ${condition.upBw} delay ${condition.delay} plr ${condition.plr}`);

      // Clear existing pf rules and load new ones
      await exec('sudo pfctl -f /etc/pf.conf 2>/dev/null');
      await exec('echo "dummynet out quick proto tcp from any to any pipe 1" | sudo pfctl -a conditioning -f -');
      await exec('echo "dummynet in quick proto tcp from any to any pipe 2" | sudo pfctl -a conditioning -f -');

      // Enable pf
      await exec('sudo pfctl -e 2>/dev/null');
      console.log(
        `Condition '${condition.name}' applied: Down: ${condition.downBw}, Up: ${condition.upBw}, Delay: ${condition.delay}ms, Packet Loss: ${condition.plr}`
      );
    } else {
      // If no limits are specified (e.g., ideal), just reset
      await resetNetwork();
      console.log(`${condition.name} applied: No bandwidth limits, 0ms latency, 0% packet loss.`);
    }
  } catch (error) {
    console.error(`Error applying condition '${condition.name}':`, error);
    throw error;
  }
}

// Define network conditions
const networkConditions: NetworkCondition[] = [
  {
    name: 'Ideal Network',
    // No downBw, upBw, delay, or plr means no limits
  },
  {
    name: '4G Speeds',
    downBw: '20Mbit/s',
    upBw: '10Mbit/s',
    delay: '50',
    plr: '0.005',
  },
  {
    name: '3G Speeds',
    downBw: '1Mbit/s',
    upBw: '0.5Mbit/s',
    delay: '100',
    plr: '0.01',
  },
  {
    name: 'High Packet Loss',
    downBw: '5Mbit/s',
    upBw: '2Mbit/s',
    delay: '200',
    plr: '0.5',
  },
  {
    name: 'Reset',
    // No limits, just resets
  },
];

// Function to apply a condition by name
async function setNetworkCondition(conditionName: string): Promise<void> {
  const condition = networkConditions.find((c) => c.name.toLowerCase().replace(/\s+/g, '-') === conditionName.toLowerCase());
  if (!condition) {
    throw new Error(`Unknown condition: ${conditionName}. Available: ${networkConditions.map(c => c.name).join(', ')}`);
  }
  if (condition.name === 'Reset') {
    await resetNetwork();
  } else {
    await applyCondition(condition);
  }
}

export { setNetworkCondition, networkConditions };