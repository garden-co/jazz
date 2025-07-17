<template>
  <div class="covalue-viewer">
    <div v-if="loading" class="loading-state">
      <p>Loading CoValue...</p>
    </div>
    
    <div v-else-if="error" class="error-state">
      <p>Error loading CoValue: {{ error }}</p>
    </div>
    
    <div v-else-if="coValueData" class="covalue-content">
      <!-- CoValue Header -->
      <div class="covalue-header">
        <h4>{{ coValueData.extendedType || coValueData.type || 'CoValue' }}</h4>
        <div class="covalue-id">
          <code>{{ coValueId }}</code>
          <button @click="copyToClipboard(coValueId)" class="copy-button" title="Copy ID">
            📋
          </button>
        </div>
      </div>
      
      <!-- Group Information (if it's a group) -->
      <div v-if="coValueData.extendedType === 'group'" class="group-info">
        <h5>Group Members & Permissions</h5>
        <div class="group-details">
          <p v-if="coValueData.snapshot?.readKey">
            <strong>Type:</strong> Collaborative Group
          </p>

          <!-- Members Table -->
          <div v-if="groupMembers.length > 0" class="members-table">
            <table>
              <thead>
                <tr>
                  <th>Account</th>
                  <th>Permission</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="member in groupMembers" :key="member.accountId">
                  <td>
                    <button
                      @click="$emit('navigate-to-covalue', member.accountId, 'Account')"
                      class="link-button account-link"
                    >
                      {{ member.accountId }}
                    </button>
                  </td>
                  <td>
                    <span class="permission-badge" :class="`permission-${member.permission}`">
                      {{ member.permission }}
                    </span>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>

          <p v-else class="no-members">
            No members found in this group.
          </p>
        </div>
      </div>
      
      <!-- Account Information (if it's an account) -->
      <div v-if="coValueData.extendedType === 'account'" class="account-info">
        <h5>Account Information</h5>
        <div class="account-details">
          <p v-if="coValueData.snapshot?.profile?.name">
            <strong>Name:</strong> {{ coValueData.snapshot.profile.name }}
          </p>
          <p v-if="coValueData.snapshot?.root">
            <strong>Root:</strong>
            <button 
              @click="$emit('navigate-to-covalue', coValueData.snapshot.root, 'Account Root')"
              class="link-button"
            >
              {{ coValueData.snapshot.root }}
            </button>
          </p>
        </div>
      </div>
      
      <!-- Properties Grid -->
      <div v-if="coValueData.properties?.length" class="properties-grid">
        <h5>Properties</h5>
        <div class="properties-list">
          <div 
            v-for="prop in coValueData.properties" 
            :key="prop.key"
            class="property-item"
          >
            <div class="property-key">{{ prop.key }}</div>
            <div class="property-value">
              <button 
                v-if="prop.isCoValueRef"
                @click="$emit('navigate-to-covalue', prop.value, `${prop.key}`)"
                class="link-button"
              >
                {{ prop.value }}
              </button>
              <span v-else>{{ formatValue(prop.value) }}</span>
            </div>
          </div>
        </div>
      </div>
      
      <!-- Raw Data -->
      <div class="raw-data-section">
        <h5>Raw Data</h5>
        <div class="raw-data-content">
          <pre>{{ JSON.stringify(coValueData.snapshot, null, 2) }}</pre>
          <button @click="copyToClipboard(JSON.stringify(coValueData.snapshot, null, 2))" class="copy-button">
            Copy Raw Data
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { LocalNode } from "cojson";
import { ref, watch } from "vue";
import { useCoValueResolver } from "../composables/useCoValueResolver.js";

interface Props {
  coValueId: string;
  localNode?: LocalNode;
}

const props = defineProps<Props>();

const emit = defineEmits<{
  "navigate-to-covalue": [id: string, title: string];
}>();

// Use the CoValue resolver
const { loading, error, resolveCoValue } = useCoValueResolver(props.localNode);
const coValueData = ref<any>(null);
const groupMembers = ref<Array<{ accountId: string; permission: string }>>([]);

// Load CoValue using the resolver
const loadCoValue = async (id: string) => {
  try {
    const resolved = await resolveCoValue(id);
    coValueData.value = resolved;

    // Extract group members if it's a group
    if (
      resolved?.extendedType === "group" &&
      resolved.snapshot &&
      typeof resolved.snapshot === "object"
    ) {
      extractGroupMembers(resolved.snapshot);
    } else {
      groupMembers.value = [];
    }
  } catch (err) {
    console.error("Failed to load CoValue:", err);
  }
};

// Extract group members from snapshot (like React Inspector)
const extractGroupMembers = (snapshot: any) => {
  const members: Array<{ accountId: string; permission: string }> = [];

  // Look for member entries in the snapshot
  for (const [key, value] of Object.entries(snapshot)) {
    // Skip non-member keys
    if (
      key === "readKey" ||
      key.startsWith("sealer_") ||
      key.startsWith("key_")
    ) {
      continue;
    }

    // Check if this looks like a member entry (account ID)
    if (
      typeof key === "string" &&
      key.startsWith("co_") &&
      typeof value === "string"
    ) {
      // Map Jazz permission values to display names
      let permission = value as string;
      if (permission === "everyone") permission = "reader";
      if (permission === "writer") permission = "writer";
      if (permission === "admin") permission = "admin";

      members.push({
        accountId: key,
        permission: permission,
      });
    }
  }

  groupMembers.value = members;
};

// Format values for display
const formatValue = (value: any): string => {
  if (typeof value === "object" && value !== null) {
    return JSON.stringify(value);
  }
  return String(value);
};

// Copy to clipboard function
const copyToClipboard = async (text: string) => {
  try {
    await navigator.clipboard.writeText(text);
  } catch (err) {
    console.error("Failed to copy to clipboard:", err);
  }
};

// Watch for coValueId changes
watch(
  () => props.coValueId,
  (newId) => {
    if (newId) {
      loadCoValue(newId);
    }
  },
  { immediate: true },
);
</script>

<style scoped>
.covalue-viewer {
  padding: 1rem;
}

.loading-state, .error-state {
  text-align: center;
  padding: 2rem;
  color: var(--j-text-color);
}

.error-state {
  color: #dc2626;
}

.covalue-header {
  margin-bottom: 1rem;
  padding-bottom: 0.75rem;
  border-bottom: 1px solid var(--j-border-color);
}

.covalue-header h4 {
  margin: 0 0 0.5rem 0;
  color: var(--j-primary-color);
  font-size: 1.25rem;
}

.covalue-id {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  font-size: 0.875rem;
  color: var(--j-text-color);
}

.copy-button {
  background: none;
  border: 1px solid var(--j-border-color);
  border-radius: var(--j-radius-sm);
  padding: 0.25rem 0.5rem;
  cursor: pointer;
  font-size: 0.75rem;
  color: var(--j-text-color);
}

.copy-button:hover {
  background: var(--j-foreground);
}

.group-info, .account-info {
  margin-bottom: 1.5rem;
}

.group-info h5, .account-info h5, .properties-grid h5, .raw-data-section h5 {
  margin: 0 0 0.75rem 0;
  color: var(--j-text-color-strong);
  font-size: 1rem;
  font-weight: 600;
}

.group-details, .account-details {
  margin-bottom: 1rem;
}

.group-details p, .account-details p {
  margin: 0.5rem 0;
  color: var(--j-text-color);
}

.link-button {
  background: none;
  border: none;
  color: var(--j-primary-color);
  cursor: pointer;
  text-decoration: underline;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  font-size: 0.875rem;
}

.link-button:hover {
  color: color-mix(in srgb, var(--j-primary-color) 80%, black);
}

.properties-list {
  display: grid;
  gap: 0.5rem;
}

.property-item {
  display: grid;
  grid-template-columns: 1fr 2fr;
  gap: 1rem;
  padding: 0.75rem;
  background: var(--j-foreground);
  border: 1px solid var(--j-border-color);
  border-radius: var(--j-radius-md);
  align-items: center;
}

.property-key {
  font-weight: 500;
  color: var(--j-text-color-strong);
}

.property-value {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  font-size: 0.875rem;
  color: var(--j-text-color);
}

.raw-data-section {
  margin-top: 1.5rem;
}

.raw-data-content {
  position: relative;
  background: var(--j-foreground);
  border: 1px solid var(--j-border-color);
  border-radius: var(--j-radius-md);
  padding: 1rem;
}

.raw-data-content pre {
  margin: 0;
  font-size: 0.75rem;
  color: var(--j-text-color);
  white-space: pre-wrap;
  word-break: break-all;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
}

.raw-data-content .copy-button {
  position: absolute;
  top: 0.5rem;
  right: 0.5rem;
}

/* Members table styling */
.members-table {
  margin-top: 1rem;
  border: 1px solid var(--j-border-color);
  border-radius: var(--j-radius-md);
  overflow: hidden;
}

.members-table table {
  width: 100%;
  border-collapse: collapse;
}

.members-table th,
.members-table td {
  padding: 0.75rem;
  text-align: left;
  border-bottom: 1px solid var(--j-border-color);
}

.members-table th {
  background-color: var(--j-foreground);
  font-weight: 600;
  color: var(--j-text-color-strong);
  font-size: 0.875rem;
}

.members-table td {
  background-color: var(--j-background);
  color: var(--j-text-color);
}

.members-table tr:last-child td {
  border-bottom: none;
}

.account-link {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  font-size: 0.75rem;
  color: var(--j-primary-color);
  text-decoration: none;
  background: none;
  border: none;
  cursor: pointer;
  padding: 0;
}

.account-link:hover {
  text-decoration: underline;
}

.permission-badge {
  display: inline-block;
  padding: 0.25rem 0.5rem;
  border-radius: var(--j-radius-sm);
  font-size: 0.75rem;
  font-weight: 500;
  text-transform: capitalize;
}

.permission-reader {
  background-color: color-mix(in srgb, #3b82f6 20%, var(--j-background));
  color: #3b82f6;
  border: 1px solid color-mix(in srgb, #3b82f6 30%, transparent);
}

.permission-writer {
  background-color: color-mix(in srgb, #f59e0b 20%, var(--j-background));
  color: #f59e0b;
  border: 1px solid color-mix(in srgb, #f59e0b 30%, transparent);
}

.permission-admin {
  background-color: color-mix(in srgb, #ef4444 20%, var(--j-background));
  color: #ef4444;
  border: 1px solid color-mix(in srgb, #ef4444 30%, transparent);
}

.no-members {
  color: var(--j-text-color);
  font-style: italic;
  margin-top: 1rem;
}
</style>
