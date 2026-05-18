import SwiftUI

@main
struct VCOMenuBarApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) var appDelegate
    @StateObject private var controller = PipelineController()

    var body: some Scene {
        MenuBarExtra("VCO", systemImage: "film.stack") {
            MenuBarView(controller: controller)
        }
        .menuBarExtraStyle(.window)
    }

    init() {
        // Store controller reference for AppDelegate to use
        AppDelegate.controllerFactory = { [self] in self.controller }
    }
}

class AppDelegate: NSObject, NSApplicationDelegate {
    static var controllerFactory: (() -> PipelineController)?
    private var controller: PipelineController?

    func applicationDidFinishLaunching(_ notification: Notification) {
        // Delay slightly to ensure @StateObject is initialized
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) {
            guard let ctrl = AppDelegate.controllerFactory?() else { return }
            self.controller = ctrl
            Task { @MainActor in
                ctrl.notificationManager.requestPermission()
                await ctrl.restoreOnLaunch()
            }
        }
    }
}
