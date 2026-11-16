import XCTest

/// The app on the iOS simulator: adding a task shows it in the list, through
/// JazzQuery.
final class StagePlanUITests: XCTestCase {
    @MainActor
    func testAddingATaskShowsIt() throws {
        let app = XCUIApplication()
        app.launch()
        let add = app.buttons["Add task"]
        XCTAssertTrue(add.waitForExistence(timeout: 30), "the app should open its tasks")
        XCTAssertTrue(app.staticTexts["No tasks yet"].waitForExistence(timeout: 10))
        add.tap()
        XCTAssertTrue(app.staticTexts["Task 1"].waitForExistence(timeout: 10))
        add.tap()
        XCTAssertTrue(app.staticTexts["Task 2"].waitForExistence(timeout: 10))
    }
}
