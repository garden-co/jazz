#!/usr/bin/env node

/**
 * Migration script to update all vercel.json files to use the enhanced ignore script
 */

import { readFileSync, writeFileSync } from 'fs';
import { glob } from 'glob';

console.log('🔄 Migrating Vercel projects to use enhanced ignore script...');

// Find all vercel.json files
const vercelFiles = glob.sync('**/vercel.json', {
  ignore: ['node_modules/**']
});

console.log(`📁 Found ${vercelFiles.length} vercel.json files:`);

for (const file of vercelFiles) {
  console.log(`  - ${file}`);
  
  try {
    const content = readFileSync(file, 'utf8');
    const config = JSON.parse(content);
    
    // Update the ignoreCommand to use the enhanced script
    if (config.ignoreCommand) {
      const oldCommand = config.ignoreCommand;
      config.ignoreCommand = oldCommand.replace(
        'ignore-vercel-build.js',
        'ignore-vercel-build-enhanced.js'
      );
      
      // Write back the updated configuration
      writeFileSync(file, JSON.stringify(config, null, 2) + '\n');
      
      console.log(`    ✅ Updated: ${oldCommand} → ${config.ignoreCommand}`);
    } else {
      console.log(`    ⚠️  No ignoreCommand found in ${file}`);
    }
  } catch (error) {
    console.log(`    ❌ Error processing ${file}: ${error.message}`);
  }
}

console.log('\n🎉 Migration completed!');
console.log('\n📋 Next steps:');
console.log('1. Test the enhanced ignore script with a small change');
console.log('2. Monitor deployment logs to ensure it works correctly');
console.log('3. Consider enabling Turbo remote cache in Vercel dashboard');
console.log('4. Remove the old ignore-vercel-build.js when confident');
