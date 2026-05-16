import SwiftUI

@main
struct VCOMenuBarApp: App {
    @StateObject private var controller = PipelineController()

    var body: some Scene {
        MenuBarExtra("VCO", systemImage: "film.stack") {
            MenuBarView(controller: controller)
        }
        .menuBarExtraStyle(.window)
    }

    init() {
        Task { @MainActor in
            let ctrl = PipelineController()
            ctrl.notificationManager.requestPermission()
            await ctrl.restoreOnLaunch()
        }
    }
}
