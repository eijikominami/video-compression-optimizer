import Foundation
import UserNotifications

protocol NotificationManaging {
    func requestPermission()
    func sendPipelineComplete(successful: Int, failed: Int)
    func sendAuthExpired()
    func sendDiskSpaceInsufficient()
    func sendAllFilesFailed()
    func sendDeleteOriginalFailed(filename: String)
}

class NotificationManager: NotificationManaging {
    private var center: UNUserNotificationCenter?

    init() {
        if Bundle.main.bundleIdentifier != nil {
            center = UNUserNotificationCenter.current()
        }
    }

    func requestPermission() {
        center?.requestAuthorization(options: [.alert, .sound]) { _, _ in }
    }

    func sendPipelineComplete(successful: Int, failed: Int) {
        send(title: "VCO パイプライン完了", body: "成功: \(successful), 失敗: \(failed)")
    }

    func sendAuthExpired() {
        send(title: "VCO 認証エラー", body: "AWS 認証が期限切れです。aws sso login を実行してください。")
    }

    func sendDiskSpaceInsufficient() {
        send(title: "VCO ディスク容量不足", body: "ディスク容量が不足しています。")
    }

    func sendAllFilesFailed() {
        send(title: "VCO 全件失敗", body: "パイプライン内の全ファイルが失敗しました。")
    }

    func sendDeleteOriginalFailed(filename: String) {
        send(title: "VCO 削除失敗", body: "\(filename) のオリジナル削除に失敗しました。")
    }

    private func send(title: String, body: String) {
        guard let center else { return }
        let content = UNMutableNotificationContent()
        content.title = title
        content.body = body
        content.sound = .default
        let request = UNNotificationRequest(
            identifier: UUID().uuidString, content: content,
            trigger: nil
        )
        center.add(request)
    }
}
