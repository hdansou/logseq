import { expect } from '@playwright/test'
import fs from 'fs/promises'
import path from 'path'
import { test } from './fixtures'
import { randomString, createRandomPage } from './utils'

test('render app', async ({ page }) => {
  // NOTE: part of app startup tests is moved to `fixtures.ts`.
  await page.waitForFunction('window.document.title != "Loading"')

  expect(await page.title()).toMatch(/^Logseq.*?/)
})

test('toggle sidebar', async ({ page }) => {
  let sidebar = page.locator('#left-sidebar')

  // Left sidebar is toggled by `is-open` class
  if (/is-open/.test(await sidebar.getAttribute('class'))) {
    await page.click('#left-menu.button')
    await expect(sidebar).not.toHaveClass(/is-open/)
  } else {
    await page.click('#left-menu.button')
    await page.waitForTimeout(10)
    await expect(sidebar).toHaveClass(/is-open/)
    await page.click('#left-menu.button')
    await page.waitForTimeout(10)
    await expect(sidebar).not.toHaveClass(/is-open/)
  }

  await page.click('#left-menu.button')

  await page.waitForTimeout(10)
  await expect(sidebar).toHaveClass(/is-open/)
  await page.waitForSelector('#left-sidebar .left-sidebar-inner', { state: 'visible' })
  await page.waitForSelector('#left-sidebar a:has-text("New page")', { state: 'visible' })
})

test('search', async ({ page }) => {
  await page.click('#search-button')
  await page.waitForSelector('[placeholder="Search or create page"]')
  await page.fill('[placeholder="Search or create page"]', 'welcome')

  await page.waitForTimeout(500)
  const results = await page.$$('#ui__ac-inner .block')
  expect(results.length).toBeGreaterThanOrEqual(1)
})

test('create page and blocks, save to disk', async ({ page, block, graphDir }) => {
  const pageTitle = await createRandomPage(page)

  // do editing
  await block.mustFill('this is my first bullet')
  await block.enterNext()

  await block.waitForBlocks(1)

  await block.mustFill('this is my second bullet')
  await block.clickNext()

  await block.mustFill('this is my third bullet')
  await block.indent()
  await block.enterNext()

  await page.keyboard.type('continue editing test')
  await page.keyboard.press('Shift+Enter')
  await page.keyboard.type('continue')

  await block.enterNext()
  expect(await block.unindent()).toBe(true)
  expect(await block.unindent()).toBe(false)
  await block.mustFill('test ok')
  await page.keyboard.press('Escape')

  await block.waitForBlocks(5)

  // active edit, and create next block
  await block.clickNext()
  await page.fill('textarea >> nth=0', 'test')
  for (let i = 0; i < 5; i++) {
    await page.keyboard.press('Backspace', { delay: 100 })
  }

  await page.keyboard.press('Escape')
  await block.waitForBlocks(5)

  await page.waitForTimeout(2000) // wait for saving to disk
  const contentOnDisk = await fs.readFile(
    path.join(graphDir, `pages/${pageTitle}.md`),
    'utf8'
  )
  expect(contentOnDisk.trim()).toEqual(`
- this is my first bullet
- this is my second bullet
	- this is my third bullet
	- continue editing test
	  continue
- test ok`.trim())
})


test('delete and backspace', async ({ page, block }) => {
  await createRandomPage(page)

  await block.mustFill('test')

  // backspace
  await page.keyboard.press('Backspace')
  await page.keyboard.press('Backspace')
  expect(await page.inputValue('textarea >> nth=0')).toBe('te')

  // refill
  await block.enterNext()
  await page.type('textarea >> nth=0', 'test', { delay: 50 })
  await page.keyboard.press('ArrowLeft', { delay: 50 })
  await page.keyboard.press('ArrowLeft', { delay: 50 })

  // delete
  await page.keyboard.press('Delete', { delay: 50 })
  expect(await page.inputValue('textarea >> nth=0')).toBe('tet')
  await page.keyboard.press('Delete', { delay: 50 })
  expect(await page.inputValue('textarea >> nth=0')).toBe('te')
  await page.keyboard.press('Delete', { delay: 50 })
  expect(await page.inputValue('textarea >> nth=0')).toBe('te')

  // TODO: test delete & backspace across blocks
})


test('selection', async ({ page, block }) => {
  await createRandomPage(page)

  // add 5 blocks
  await block.mustFill('line 1')
  await block.enterNext()
  await block.mustFill('line 2')
  await block.enterNext()
  expect(await block.indent()).toBe(true)
  await block.mustFill('line 3')
  await block.enterNext()
  await block.mustFill('line 4')
  expect(await block.indent()).toBe(true)
  await block.enterNext()
  await block.mustFill('line 5')

  // shift+up select 3 blocks
  await page.keyboard.down('Shift')
  await page.keyboard.press('ArrowUp')
  await page.keyboard.press('ArrowUp')
  await page.keyboard.press('ArrowUp')
  await page.keyboard.up('Shift')

  await block.waitForSelectedBlocks(3)
  await page.keyboard.press('Backspace')

  await block.waitForBlocks(2)
})

test('template', async ({ page, block }) => {
  const randomTemplate = randomString(10)

  await createRandomPage(page)

  await block.mustFill('template test\ntemplate:: ' + randomTemplate)
  await block.clickNext()

  expect(await block.indent()).toBe(true)

  await block.mustFill('line1')
  await block.enterNext()
  await block.mustFill('line2')
  await block.enterNext()

  expect(await block.indent()).toBe(true)
  await block.mustFill('line3')
  await block.enterNext()

  expect(await block.unindent()).toBe(true)
  expect(await block.unindent()).toBe(true)
  expect(await block.unindent()).toBe(false) // already at the first level

  await block.waitForBlocks(4)

  // NOTE: use delay to type slower, to trigger auto-completion UI.
  await block.mustType('/template')

  await page.click('[title="Insert a created template here"]')
  // type to search template name
  await page.keyboard.type(randomTemplate.substring(0, 3), { delay: 100 })

  const popupMenuItem = page.locator('.absolute >> text=' + randomTemplate)
  await popupMenuItem.waitFor({ timeout: 2000 }) // wait for template search
  await popupMenuItem.click()

  await block.waitForBlocks(8)
})

test('auto completion square brackets', async ({ page }) => {
  await createRandomPage(page)

  // In this test, `type` is unsed instead of `fill`, to allow for auto-completion.

  // [[]]
  await page.type('textarea >> nth=0', 'This is a [')
  await expect(page.locator('textarea >> nth=0')).toHaveText('This is a []')
  await page.waitForTimeout(100)
  await page.type('textarea >> nth=0', '[')
  // wait for search popup
  await page.waitForSelector('text="Search for a page"')

  expect(await page.inputValue('textarea >> nth=0')).toBe('This is a [[]]')

  // re-enter edit mode
  await page.press('textarea >> nth=0', 'Escape')
  await page.click('.ls-block >> nth=-1')
  await page.waitForSelector('textarea >> nth=0', { state: 'visible' })

  // #3253
  await page.press('textarea >> nth=0', 'ArrowLeft')
  await page.press('textarea >> nth=0', 'ArrowLeft')
  await page.press('textarea >> nth=0', 'Enter')
  await page.waitForSelector('text="Search for a page"', { state: 'visible' })

  // type more `]`s
  await page.type('textarea >> nth=0', ']')
  expect(await page.inputValue('textarea >> nth=0')).toBe('This is a [[]]')
  await page.type('textarea >> nth=0', ']')
  expect(await page.inputValue('textarea >> nth=0')).toBe('This is a [[]]')
  await page.type('textarea >> nth=0', ']')
  expect(await page.inputValue('textarea >> nth=0')).toBe('This is a [[]]]')
})

test('auto completion and auto pair', async ({ page, block }) => {
  await createRandomPage(page)

  await block.mustFill('Auto-completion test')
  await block.enterNext()

  // {{
  await page.type('textarea >> nth=0', 'type {{')
  expect(await page.inputValue('textarea >> nth=0')).toBe('type {{}}')

  // ((
  await block.clickNext()

  await page.type('textarea >> nth=0', 'type (')
  expect(await page.inputValue('textarea >> nth=0')).toBe('type ()')
  await page.type('textarea >> nth=0', '(')
  expect(await page.inputValue('textarea >> nth=0')).toBe('type (())')

  // 99  #3444
  // TODO: Test under different keyboard layout when Playwright supports it
  // await block.clickNext()

  // await page.type('textarea >> nth=0', 'type 9')
  // expect(await page.inputValue('textarea >> nth=0')).toBe('type 9')
  // await page.type('textarea >> nth=0', '9')
  // expect(await page.inputValue('textarea >> nth=0')).toBe('type 99')

  // [[  #3251
  await block.clickNext()

  await page.type('textarea >> nth=0', 'type [')
  expect(await page.inputValue('textarea >> nth=0')).toBe('type []')
  await page.type('textarea >> nth=0', '[')
  expect(await page.inputValue('textarea >> nth=0')).toBe('type [[]]')
  await page.press('textarea >> nth=0', 'Escape') // escape any popup from `[[]]`

  // ``
  await block.clickNext()

  await page.type('textarea >> nth=0', 'type `')
  expect(await page.inputValue('textarea >> nth=0')).toBe('type ``')
  await page.type('textarea >> nth=0', 'code here')

  expect(await page.inputValue('textarea >> nth=0')).toBe('type `code here`')
})

test('invalid page props #3944', async ({ page, block }) => {
  await createRandomPage(page)

  await block.mustFill('public:: true\nsize:: 65535')
  await page.press('textarea >> nth=0', 'Enter')
  // Force rendering property block
  await block.clickNext()
})
