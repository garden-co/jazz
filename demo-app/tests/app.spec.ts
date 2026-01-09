import { test, expect } from '@playwright/test';

test.describe('Issue Tracker App', () => {
  test.beforeEach(async ({ page }) => {
    // Use non-persistent mode to start fresh each test
    await page.goto('/?persist=false');
    // Wait for app to initialize and fake data to load
    await expect(page.getByRole('heading', { name: 'Issue Tracker' })).toBeVisible({ timeout: 15000 });
    // Wait for issues to load
    await expect(page.getByRole('button', { name: /All Issues/ })).toBeVisible();
  });

  test('displays initial state with fake data', async ({ page }) => {
    // Sidebar should be visible
    await expect(page.getByRole('heading', { name: 'Issue Tracker' })).toBeVisible();

    // All Issues and My Issues buttons should exist
    await expect(page.getByRole('button', { name: /All Issues/ })).toBeVisible();
    await expect(page.getByRole('button', { name: 'My Issues' })).toBeVisible();

    // Projects section should be visible
    await expect(page.getByText('Projects')).toBeVisible();

    // Filter bar should be visible
    await expect(page.getByText('Filters:')).toBeVisible();

    // New Issue button in header
    await expect(page.getByRole('button', { name: 'New Issue' })).toBeVisible();
  });

  test('shows projects in sidebar', async ({ page }) => {
    // Default projects from fake data
    await expect(page.getByRole('button', { name: /Frontend/ })).toBeVisible();
    await expect(page.getByRole('button', { name: /Backend/ })).toBeVisible();
    await expect(page.getByRole('button', { name: /Infrastructure/ })).toBeVisible();
  });

  test('filters by project when clicking sidebar', async ({ page }) => {
    // Click on Frontend project
    await page.getByRole('button', { name: /Frontend/ }).click();

    // Button should now be selected (secondary variant)
    await expect(page.getByRole('button', { name: /Frontend/ })).toHaveClass(/secondary/);
  });

  test('toggles My Issues view', async ({ page }) => {
    // Click My Issues
    await page.getByRole('button', { name: 'My Issues' }).click();

    // Button should be selected
    await expect(page.getByRole('button', { name: 'My Issues' })).toHaveClass(/secondary/);
  });

  test('creates a new issue', async ({ page }) => {
    // Get initial issue count
    const allIssuesButton = page.getByRole('button', { name: /All Issues/ });
    const initialCount = await allIssuesButton.textContent();
    const initialNumber = parseInt(initialCount?.match(/\d+/)?.[0] || '0');

    // Open the issue form
    await page.getByRole('button', { name: 'New Issue' }).click();

    // Form dialog should appear
    await expect(page.getByRole('dialog', { name: 'New Issue' })).toBeVisible({ timeout: 5000 });

    // Fill in the title
    await page.getByPlaceholder('Issue title...').fill('Test Issue from E2E');
    await page.getByPlaceholder('Describe the issue...').fill('This is a test description');

    // Submit
    await page.getByRole('button', { name: 'Create Issue' }).click();

    // Wait for dialog to close
    await expect(page.getByRole('dialog', { name: 'New Issue' })).not.toBeVisible({ timeout: 5000 });

    // Verify issue was created by checking count increased
    await expect(allIssuesButton).toContainText(String(initialNumber + 1), { timeout: 5000 });
  });

  test('cancels issue creation', async ({ page }) => {
    await page.getByRole('button', { name: 'New Issue' }).click();
    await expect(page.getByRole('dialog', { name: 'New Issue' })).toBeVisible({ timeout: 5000 });

    await page.getByPlaceholder('Issue title...').fill('Should not be created');
    await page.getByRole('button', { name: 'Cancel' }).click();

    // Dialog should close
    await expect(page.getByRole('dialog', { name: 'New Issue' })).not.toBeVisible({ timeout: 5000 });

    // Issue should not exist
    await expect(page.getByText('Should not be created')).not.toBeVisible();
  });
});

test.describe('Filter Bar', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/?persist=false');
    await expect(page.getByRole('heading', { name: 'Issue Tracker' })).toBeVisible({ timeout: 15000 });
    await expect(page.getByRole('button', { name: /All Issues/ })).toBeVisible();
  });

  test('filters by status', async ({ page }) => {
    // Get all comboboxes - status is the first one
    const comboboxes = page.getByRole('combobox');
    const statusTrigger = comboboxes.nth(0);

    // Verify initial state
    await expect(statusTrigger).toContainText('All Statuses');

    await statusTrigger.click();
    await page.getByRole('option', { name: 'In Progress' }).click();

    // The trigger should now show "In Progress"
    await expect(statusTrigger).toContainText('In Progress');
  });

  test('filters by priority', async ({ page }) => {
    // Priority is the second combobox
    const comboboxes = page.getByRole('combobox');
    const priorityTrigger = comboboxes.nth(1);

    await expect(priorityTrigger).toContainText('All Priorities');

    await priorityTrigger.click();
    await page.getByRole('option', { name: 'High' }).click();

    await expect(priorityTrigger).toContainText('High');
  });

  test('filters by assignee', async ({ page }) => {
    // Assignee is the third combobox
    const comboboxes = page.getByRole('combobox');
    const assigneeTrigger = comboboxes.nth(2);

    await expect(assigneeTrigger).toContainText('All Assignees');

    await assigneeTrigger.click();
    await page.getByRole('option', { name: 'Alice Chen' }).click();

    await expect(assigneeTrigger).toContainText('Alice Chen');
  });

  test('filters by label', async ({ page }) => {
    // Label is the fourth combobox
    const comboboxes = page.getByRole('combobox');
    const labelTrigger = comboboxes.nth(3);

    await expect(labelTrigger).toContainText('All Labels');

    await labelTrigger.click();
    await page.getByRole('option', { name: 'bug' }).click();

    await expect(labelTrigger).toContainText('bug');
  });

  test('clears all filters', async ({ page }) => {
    const comboboxes = page.getByRole('combobox');

    // Apply a status filter
    await comboboxes.nth(0).click();
    await page.getByRole('option', { name: 'Done' }).click();

    // Apply a priority filter
    await comboboxes.nth(1).click();
    await page.getByRole('option', { name: 'Urgent' }).click();

    // Clear button should appear
    await expect(page.getByRole('button', { name: 'Clear' })).toBeVisible();

    // Click clear
    await page.getByRole('button', { name: 'Clear' }).click();

    // Filters should be reset
    await expect(comboboxes.nth(0)).toContainText('All Statuses');
    await expect(comboboxes.nth(1)).toContainText('All Priorities');

    // Clear button should be hidden
    await expect(page.getByRole('button', { name: 'Clear' })).not.toBeVisible();
  });

  test('combines multiple filters', async ({ page }) => {
    const comboboxes = page.getByRole('combobox');

    // Apply status filter
    await comboboxes.nth(0).click();
    await page.getByRole('option', { name: 'Todo' }).click();

    // Apply priority filter
    await comboboxes.nth(1).click();
    await page.getByRole('option', { name: 'Medium' }).click();

    // Both should be applied
    await expect(comboboxes.nth(0)).toContainText('Todo');
    await expect(comboboxes.nth(1)).toContainText('Medium');

    // Clear should appear
    await expect(page.getByRole('button', { name: 'Clear' })).toBeVisible();
  });
});

test.describe('Issue List', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/?persist=false&items=50');
    await expect(page.getByRole('heading', { name: 'Issue Tracker' })).toBeVisible({ timeout: 15000 });
    await expect(page.getByRole('button', { name: /All Issues/ })).toBeVisible();
  });

  test('shows pagination for many issues', async ({ page }) => {
    // With 50 items and default page size of 20, should show pagination
    await expect(page.getByText(/Showing \d+-\d+ of \d+/)).toBeVisible();
    await expect(page.getByRole('button', { name: 'Next' })).toBeVisible();
    await expect(page.getByRole('button', { name: 'Previous' })).toBeVisible();
  });

  test('navigates between pages', async ({ page }) => {
    // Initial state: Previous disabled, Next enabled
    await expect(page.getByRole('button', { name: 'Previous' })).toBeDisabled();
    await expect(page.getByRole('button', { name: 'Next' })).toBeEnabled();

    // Click Next
    await page.getByRole('button', { name: 'Next' }).click();

    // Now Previous should be enabled
    await expect(page.getByRole('button', { name: 'Previous' })).toBeEnabled();

    // Text should show different range
    await expect(page.getByText(/Showing 21-/)).toBeVisible();

    // Go back
    await page.getByRole('button', { name: 'Previous' }).click();
    await expect(page.getByText(/Showing 1-/)).toBeVisible();
  });

  test('resets pagination when filter changes', async ({ page }) => {
    // Go to page 2
    await page.getByRole('button', { name: 'Next' }).click();
    await expect(page.getByText(/Showing 21-/)).toBeVisible();

    // Apply a filter - clicking any filter option should reset pagination
    const statusTrigger = page.getByRole('combobox').nth(0);
    await statusTrigger.click();
    await page.getByRole('option', { name: 'In Progress' }).click();

    // Wait for filter to apply
    await page.waitForTimeout(500);

    // After filtering, either:
    // 1. Shows "Showing 1-X of Y" (results exist, page reset to 1)
    // 2. Shows "No issues found" (filter eliminated all results)
    // 3. No pagination shown at all (fewer than 20 results)
    // The key test is that we're no longer on page 2 (showing 21-)
    await expect(page.getByText(/Showing 21-/)).not.toBeVisible();
  });
});

test.describe('Issue Detail', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/?persist=false');
    await expect(page.getByRole('heading', { name: 'Issue Tracker' })).toBeVisible({ timeout: 15000 });
    await expect(page.getByRole('button', { name: /All Issues/ })).toBeVisible();
    // Wait for issues to render
    await page.waitForTimeout(1000);
  });

  test('opens issue detail when clicking an issue', async ({ page }) => {
    // Look for issue rows containing issue titles from the fake data
    // Issues have text like "Fix...", "Add...", etc.
    const issueText = page.getByText(/Fix login button|Add dark mode|Optimize database/);
    await issueText.first().click();

    // Wait for dialog to open
    await expect(page.getByRole('dialog')).toBeVisible({ timeout: 5000 });
  });
});

test.describe('Theme Toggle', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/?persist=false');
    await expect(page.getByRole('heading', { name: 'Issue Tracker' })).toBeVisible({ timeout: 15000 });
  });

  test('toggles dark mode', async ({ page }) => {
    // Find the theme toggle button in header (icon-only button without text)
    const header = page.locator('header');
    // The theme button is an icon-only button in the header's right section
    const buttons = header.getByRole('button');
    // It's one of the buttons that's not "New Issue"
    const themeButton = buttons.filter({ hasNot: page.getByText('New Issue') }).first();

    // Get initial state
    const htmlElement = page.locator('html');
    const initialIsDark = await htmlElement.evaluate(el => el.classList.contains('dark'));

    // Click to toggle
    await themeButton.click();

    // State should be opposite
    const newIsDark = await htmlElement.evaluate(el => el.classList.contains('dark'));
    expect(newIsDark).toBe(!initialIsDark);

    // Toggle back
    await themeButton.click();
    const finalIsDark = await htmlElement.evaluate(el => el.classList.contains('dark'));
    expect(finalIsDark).toBe(initialIsDark);
  });
});

test.describe('Sidebar Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/?persist=false');
    await expect(page.getByRole('heading', { name: 'Issue Tracker' })).toBeVisible({ timeout: 15000 });
    await expect(page.getByRole('button', { name: /All Issues/ })).toBeVisible();
  });

  test('selects All Issues by default', async ({ page }) => {
    await expect(page.getByRole('button', { name: /All Issues/ })).toHaveClass(/secondary/);
  });

  test('switches between All Issues and My Issues', async ({ page }) => {
    // Click My Issues
    await page.getByRole('button', { name: 'My Issues' }).click();
    await expect(page.getByRole('button', { name: 'My Issues' })).toHaveClass(/secondary/);
    await expect(page.getByRole('button', { name: /All Issues/ })).not.toHaveClass(/secondary/);

    // Click back to All Issues
    await page.getByRole('button', { name: /All Issues/ }).click();
    await expect(page.getByRole('button', { name: /All Issues/ })).toHaveClass(/secondary/);
    await expect(page.getByRole('button', { name: 'My Issues' })).not.toHaveClass(/secondary/);
  });

  test('selects a project and deselects All Issues', async ({ page }) => {
    // Click Frontend project
    await page.getByRole('button', { name: /Frontend/ }).click();

    // Frontend should be selected
    await expect(page.getByRole('button', { name: /Frontend/ })).toHaveClass(/secondary/);

    // All Issues should no longer be selected
    await expect(page.getByRole('button', { name: /All Issues/ })).not.toHaveClass(/secondary/);
  });

  test('clicking All Issues deselects project', async ({ page }) => {
    // First select a project
    await page.getByRole('button', { name: /Backend/ }).click();
    await expect(page.getByRole('button', { name: /Backend/ })).toHaveClass(/secondary/);

    // Click All Issues
    await page.getByRole('button', { name: /All Issues/ }).click();

    // All Issues should be selected, Backend should not
    await expect(page.getByRole('button', { name: /All Issues/ })).toHaveClass(/secondary/);
    await expect(page.getByRole('button', { name: /Backend/ })).not.toHaveClass(/secondary/);
  });
});

test.describe('Current User', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/?persist=false');
    await expect(page.getByRole('heading', { name: 'Issue Tracker' })).toBeVisible({ timeout: 15000 });
    // Wait longer for fake data including user to load
    await page.waitForTimeout(1000);
  });

  test('displays current user in header', async ({ page }) => {
    // Header should show a user name (one of the fake users)
    // The fake data picks the first user which could be any of: Alice Chen, Bob Smith, Carol Williams, David Jones, Eve Brown
    const header = page.locator('header');
    const userNames = ['Alice Chen', 'Bob Smith', 'Carol Williams', 'David Jones', 'Eve Brown'];
    const userNameRegex = new RegExp(userNames.join('|'));
    await expect(header.getByText(userNameRegex)).toBeVisible({ timeout: 10000 });
  });
});
