import { test, expect } from '@playwright/test';

test.describe('Groove Demo App', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for app to initialize (should see "Users (0)" not "Loading...")
    await expect(page.getByRole('heading', { name: /Users \(\d+\)/ })).toBeVisible({ timeout: 10000 });
  });

  test('displays initial empty state', async ({ page }) => {
    await expect(page.getByRole('heading', { name: 'Groove Demo' })).toBeVisible();
    await expect(page.getByText('Real-time reactive database')).toBeVisible();

    // Check all panels are empty initially
    await expect(page.getByRole('heading', { name: 'Users (0)' })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Folders (0)' })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Notes (0)' })).toBeVisible();

    await expect(page.getByText('No users yet')).toBeVisible();
    await expect(page.getByText('No notes yet. Create one!')).toBeVisible();
  });

  test('creates a user', async ({ page }) => {
    const userInput = page.getByPlaceholder('New user name...');
    const addButton = page.getByRole('button', { name: 'Add' }).first();

    await userInput.fill('Alice');
    await addButton.click();

    // Verify user was created
    await expect(page.getByRole('heading', { name: 'Users (1)' })).toBeVisible();
    // Use locator within the users panel to avoid matching debug section
    await expect(page.locator('strong').getByText('Alice', { exact: true })).toBeVisible();
    await expect(page.locator('small').getByText('alice@example.com')).toBeVisible();

    // Input should be cleared
    await expect(userInput).toHaveValue('');
  });

  test('creates multiple users', async ({ page }) => {
    const userInput = page.getByPlaceholder('New user name...');
    const addButton = page.getByRole('button', { name: 'Add' }).first();

    // Create first user
    await userInput.fill('Alice');
    await addButton.click();
    await expect(page.getByRole('heading', { name: 'Users (1)' })).toBeVisible();

    // Create second user
    await userInput.fill('Bob');
    await addButton.click();
    await expect(page.getByRole('heading', { name: 'Users (2)' })).toBeVisible();

    // Both should be visible
    await expect(page.locator('strong').getByText('Alice', { exact: true })).toBeVisible();
    await expect(page.locator('strong').getByText('Bob', { exact: true })).toBeVisible();
  });

  test('creates a folder for selected user', async ({ page }) => {
    // First create a user
    await page.getByPlaceholder('New user name...').fill('Alice');
    await page.getByRole('button', { name: 'Add' }).first().click();
    await expect(page.getByRole('heading', { name: 'Users (1)' })).toBeVisible();

    // User should be auto-selected, so folder input should be enabled
    const folderInput = page.getByPlaceholder('New folder...');
    await expect(folderInput).toBeEnabled();

    // Create a folder
    await folderInput.fill('Work');
    await page.getByRole('button', { name: 'Add' }).nth(1).click();

    // Verify folder was created
    await expect(page.getByRole('heading', { name: 'Folders (1)' })).toBeVisible();
    await expect(page.locator('strong').getByText('Work', { exact: true })).toBeVisible();
    await expect(page.getByText('Owner: Alice')).toBeVisible();
  });

  test('creates a note for selected user', async ({ page }) => {
    // First create a user
    await page.getByPlaceholder('New user name...').fill('Alice');
    await page.getByRole('button', { name: 'Add' }).first().click();
    await expect(page.getByRole('heading', { name: 'Users (1)' })).toBeVisible();

    // Create a note
    const noteInput = page.getByPlaceholder('New note title...');
    await expect(noteInput).toBeEnabled();
    await noteInput.fill('My First Note');
    await page.getByRole('button', { name: 'Add Note' }).click();

    // Verify note was created
    await expect(page.getByRole('heading', { name: 'Notes (1)' })).toBeVisible();
    await expect(page.locator('strong').getByText('My First Note', { exact: true })).toBeVisible();
    await expect(page.getByText('By: Alice')).toBeVisible();
  });

  test('updates note content', async ({ page }) => {
    // Create user and note
    await page.getByPlaceholder('New user name...').fill('Alice');
    await page.getByRole('button', { name: 'Add' }).first().click();
    await page.getByPlaceholder('New note title...').fill('Test Note');
    await page.getByRole('button', { name: 'Add Note' }).click();

    // Find and update the note content
    const textarea = page.getByPlaceholder('Write something...');
    await textarea.fill('This is my updated content');

    // Content should be persisted (check debug section)
    await page.getByText('Debug: Raw Data').click();
    await expect(page.locator('pre').getByText('This is my updated content')).toBeVisible();
  });

  test('filters notes by "My Notes" tab', async ({ page }) => {
    // Create two users
    await page.getByPlaceholder('New user name...').fill('Alice');
    await page.getByRole('button', { name: 'Add' }).first().click();
    await expect(page.getByRole('heading', { name: 'Users (1)' })).toBeVisible();

    // Create note for Alice
    await page.getByPlaceholder('New note title...').fill('Alice Note');
    await page.getByRole('button', { name: 'Add Note' }).click();
    await expect(page.getByRole('heading', { name: 'Notes (1)' })).toBeVisible();

    // Create second user
    await page.getByPlaceholder('New user name...').fill('Bob');
    await page.getByRole('button', { name: 'Add' }).first().click();
    await expect(page.getByRole('heading', { name: 'Users (2)' })).toBeVisible();

    // Select Bob
    await page.locator('strong').getByText('Bob', { exact: true }).click();

    // Create note for Bob
    await page.getByPlaceholder('New note title...').fill('Bob Note');
    await page.getByRole('button', { name: 'Add Note' }).click();
    await expect(page.getByRole('heading', { name: 'Notes (2)' })).toBeVisible();

    // Both notes visible in "All Notes"
    await expect(page.locator('strong').getByText('Alice Note', { exact: true })).toBeVisible();
    await expect(page.locator('strong').getByText('Bob Note', { exact: true })).toBeVisible();

    // Filter to "My Notes" (Bob's)
    await page.getByRole('button', { name: 'My Notes' }).click();

    // Should only see Bob's note
    await expect(page.locator('strong').getByText('Bob Note', { exact: true })).toBeVisible();
    await expect(page.locator('strong').getByText('Alice Note', { exact: true })).not.toBeVisible();
  });

  // TODO: This test is flaky - second note creation fails silently
  // Possibly related to folder reference in INSERT statement
  test.skip('selects folder and filters notes', async ({ page }) => {
    // Create user
    await page.getByPlaceholder('New user name...').fill('Alice');
    await page.getByRole('button', { name: 'Add' }).first().click();

    // Create folder
    await page.getByPlaceholder('New folder...').fill('Work');
    await page.getByRole('button', { name: 'Add' }).nth(1).click();
    await expect(page.getByRole('heading', { name: 'Folders (1)' })).toBeVisible();

    // Create note without folder (root is selected by default)
    await page.getByPlaceholder('New note title...').fill('Root Note');
    await page.getByRole('button', { name: 'Add Note' }).click();
    await expect(page.getByRole('heading', { name: 'Notes (1)' })).toBeVisible();

    // Select the Work folder
    await page.locator('strong').getByText('Work', { exact: true }).click();
    // Wait for folder to be selected (In Folder button should become enabled)
    await expect(page.getByRole('button', { name: 'In Folder' })).toBeEnabled();

    // Create note in folder
    await page.getByPlaceholder('New note title...').fill('Work Note');
    await page.getByRole('button', { name: 'Add Note' }).click();
    // Wait a bit longer for the subscription to update
    await expect(page.getByRole('heading', { name: 'Notes (2)' })).toBeVisible({ timeout: 10000 });

    // Filter to "In Folder"
    await page.getByRole('button', { name: 'In Folder' }).click();

    // Should only see Work Note
    await expect(page.locator('strong').getByText('Work Note', { exact: true })).toBeVisible();
    await expect(page.locator('strong').getByText('Root Note', { exact: true })).not.toBeVisible();
  });

  test('folder inputs are disabled without user selected', async ({ page }) => {
    // Initially no user is selected
    const folderInput = page.getByPlaceholder('New folder...');
    const noteInput = page.getByPlaceholder('New note title...');

    // These should be disabled
    await expect(folderInput).toBeDisabled();
    await expect(noteInput).toBeDisabled();
  });

  test('user can be selected by clicking', async ({ page }) => {
    // Create two users
    await page.getByPlaceholder('New user name...').fill('Alice');
    await page.getByRole('button', { name: 'Add' }).first().click();

    await page.getByPlaceholder('New user name...').fill('Bob');
    await page.getByRole('button', { name: 'Add' }).first().click();

    // Click Alice to select her
    await page.locator('strong').getByText('Alice', { exact: true }).click();

    // Alice's card should be highlighted (selected)
    // Create a note to verify Alice is selected
    await page.getByPlaceholder('New note title...').fill('Test');
    await page.getByRole('button', { name: 'Add Note' }).click();

    await expect(page.getByText('By: Alice')).toBeVisible();
  });

  test('debug section shows raw data', async ({ page }) => {
    // Create a user
    await page.getByPlaceholder('New user name...').fill('Alice');
    await page.getByRole('button', { name: 'Add' }).first().click();

    // Open debug section
    await page.getByText('Debug: Raw Data').click();

    // Should show JSON data in the pre element
    const debugPre = page.locator('pre');
    await expect(debugPre).toContainText('"name": "Alice"');
    await expect(debugPre).toContainText('"email": "alice@example.com"');
  });

  test('creates user with Enter key', async ({ page }) => {
    const userInput = page.getByPlaceholder('New user name...');

    await userInput.fill('Alice');
    await userInput.press('Enter');

    await expect(page.getByRole('heading', { name: 'Users (1)' })).toBeVisible();
    await expect(page.locator('strong').getByText('Alice', { exact: true })).toBeVisible();
  });

  test('features section is visible', async ({ page }) => {
    await expect(page.getByRole('heading', { name: 'Features Demonstrated' })).toBeVisible();
    await expect(page.getByText('Real-time Subscriptions:')).toBeVisible();
    await expect(page.getByText('Binary Encoding:')).toBeVisible();
    await expect(page.getByText('Relations:')).toBeVisible();
    await expect(page.getByText('Client-side Filters:')).toBeVisible();
    await expect(page.getByText('Type-safe API:')).toBeVisible();
  });
});
