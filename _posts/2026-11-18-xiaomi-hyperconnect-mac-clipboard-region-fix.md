---
layout: post
title: "小米互联服务 Mac → 手机剪贴板失效的解决方法"
date: 2026-11-18 23:03:00 +0800
categories: [故障排查]
tags: [macOS, 小米互联, 剪贴板]
permalink: /xiaomi-hyperconnect-mac-clipboard-region-fix/
---

我在 Mac 上复制文字后，小米手机只能粘贴出手机剪贴板里的旧内容。手机复制到 Mac 倒是能用，妙享桌面也能连。两台设备连接同一个 Wi-Fi、重启两台设备、覆盖安装小米互联服务，以及备份后用新配置重新登录，都没有解决这次的故障。

这次的环境是 macOS 27.2、小米互联服务 4.0.4（构建 731）和小米 15 Ultra（澎湃 OS 4）。最后确认，问题出在两端互联服务使用的地区不同。

## 怎么找到原因

Mac 端日志显示，测试文字已经发出，发送接口返回成功。这一步只能确认 Mac 发起了发送。接上手机，用 USB 调试读取手机端日志后，才看到接收服务的实际处理结果：

```text
UniClipLyraUtil: isSameRegionWithLocal, localRegion = cn, targetRegion = us
UniClipUniversalClipDataReceiver: peer device is not same region with this device, ignore this msg
```

手机收到了这条消息，但它的地区是 `cn`，Mac 上的小米互联服务报的是 `us`，所以手机在写入剪贴板之前就丢弃了消息。Mac 系统的地区偏好是 `zh-Hans_US`，也就是简体中文界面配美国地区。

## 解决方法

先用 **⌘Q** 完全退出「小米互联服务」。在 Mac 的终端运行下面的命令，只给这个应用设置中国大陆地区，然后重新打开它。

```bash
app_prefs="$HOME/Library/Preferences/com.xiaomi.hyperConnect"
defaults write "$app_prefs" AppleLocale -string zh-Hans_CN
open -a "小米互联服务"
```

可以分别查看应用和系统的地区偏好：

```bash
defaults read "$HOME/Library/Preferences/com.xiaomi.hyperConnect" AppleLocale
defaults read -g AppleLocale
```

在我的 Mac 上，第一条返回 `zh-Hans_CN`，第二条仍返回 `zh-Hans_US`。重新打开 App 后，我复制了一段新文字，手机端日志显示两端都为 `cn`，小米笔记里也能实际粘贴出这段文字。

如果修改前没有给该 App 单独设置过 `AppleLocale`，可以退出 App 后运行下面的命令撤销。已有单独设置的话，将它改回原值即可。

```bash
defaults delete "$HOME/Library/Preferences/com.xiaomi.hyperConnect" AppleLocale
```

这是对我这套版本和地区组合的实测结果。遇到同样的单向故障时，可以先用普通文字复现，再核对接收端是否有地区不匹配的日志。
