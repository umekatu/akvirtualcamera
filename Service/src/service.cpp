/* akvirtualcamera, virtual camera for Mac and Windows.
 * Copyright (C) 2020  Gonzalo Exequiel Pedone
 *
 * akvirtualcamera is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * akvirtualcamera is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with akvirtualcamera. If not, see <http://www.gnu.org/licenses/>.
 *
 * Web-Site: http://webcamoid.github.io/
 */

#include <algorithm>
#include <condition_variable>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <thread>

#include "service.h"
#include "PlatformUtils/src/preferences.h"
#include "VCamUtils/src/logger.h"
#include "VCamUtils/src/message.h"
#include "VCamUtils/src/messageserver.h"
#include "VCamUtils/src/servicemsg.h"
#include "VCamUtils/src/videoformat.h"
#include "VCamUtils/src/videoframe.h"

namespace AkVCam
{
    struct Peer
    {
        uint64_t clientId {0};
        uint64_t pid {0};

        Peer(uint64_t clientId=0, uint64_t pid=0):
            clientId(clientId),
            pid(pid)
        {

        }
    };

    struct BroadcastSlot
    {
        Peer broadcaster;
        std::vector<Peer> listeners;
        VideoFrame frame;
        // MLFBT fix: hot frame path gets its OWN mutex + CV per device, so
        // one camera's 3 MB frame store/serialize never blocks another's.
        // m_peerMutex is reserved for the map + peer metadata only.
        std::mutex frameMutex;
        std::condition_variable_any frameAvailable;
    };

    // Held by shared_ptr so a slot survives concurrent map erase while a
    // frame thread still references it.
    typedef std::map<std::string, std::shared_ptr<BroadcastSlot>> Broadcasts;

    class ServicePrivate
    {
        public:
            MessageServer m_messageServer;

            // Broadcasting and listen
            Broadcasts m_broadcasts;
            // Guards the m_broadcasts map structure + each slot's peer
            // metadata (broadcaster / listeners). NOT the frame hot path --
            // that uses each slot's own frameMutex / frameAvailable.
            std::mutex m_peerMutex;

            ServicePrivate();
            static void removeClientById(void *userData, uint64_t clientId);
            bool clients(uint64_t clientId,
                         const Message &inMessage,
                         Message &outMessage);
            bool broadcast(uint64_t clientId,
                           const Message &inMessage,
                           Message &outMessage);
            bool listen(uint64_t clientId,
                        const Message &inMessage,
                        Message &outMessage);
    };
}

AkVCam::Service::Service()
{
    this->d = new ServicePrivate;
}

AkVCam::Service::~Service()
{
    delete this->d;
}

int AkVCam::Service::run()
{
    AkLogFunction();

    return this->d->m_messageServer.run();
}

void AkVCam::Service::stop()
{
    AkLogFunction();
    this->d->m_messageServer.stop();
}

#define BIND(member) \
    std::bind(&member, \
              this, \
              std::placeholders::_1, \
              std::placeholders::_2, \
              std::placeholders::_3)

AkVCam::ServicePrivate::ServicePrivate()
{
    AkLogFunction();

    this->m_messageServer.setPort(Preferences::servicePort());

    this->m_messageServer.subscribe(AKVCAM_SERVICE_MSG_CLIENTS  , BIND(ServicePrivate::clients)  );
    this->m_messageServer.subscribe(AKVCAM_SERVICE_MSG_BROADCAST, BIND(ServicePrivate::broadcast));
    this->m_messageServer.subscribe(AKVCAM_SERVICE_MSG_LISTEN   , BIND(ServicePrivate::listen)   );

    this->m_messageServer.connectConnectionClosed(this, &ServicePrivate::removeClientById);
}

void AkVCam::ServicePrivate::removeClientById(void *userData,
                                              uint64_t clientId)
{
    AkLogFunction();
    AkLogDebug() << "Removing client: " << clientId << std::endl;
    auto self = reinterpret_cast<ServicePrivate *>(userData);

    self->m_peerMutex.lock();
    std::string removeDevice;

    for (auto &slot: self->m_broadcasts) {
        if (slot.second->broadcaster.clientId == clientId) {
            slot.second->broadcaster = {0, 0};

            if (slot.second->listeners.empty())
                removeDevice = slot.first;

            break;
        } else {
            auto it = std::find_if(slot.second->listeners.begin(),
                                   slot.second->listeners.end(),
                                   [&clientId] (const Peer &peer) -> bool {
                return peer.clientId == clientId;
            });

            if (it != slot.second->listeners.end()) {
                slot.second->listeners.erase(it);

                if (slot.second->broadcaster.pid == 0
                    && slot.second->listeners.empty()) {
                    removeDevice = slot.first;
                }

                break;
            }
        }
    }

    if (!removeDevice.empty())
        self->m_broadcasts.erase(removeDevice);

    self->m_peerMutex.unlock();
}

bool AkVCam::ServicePrivate::clients(uint64_t clientId,
                                     const Message &inMessage,
                                     Message &outMessage)
{
    AkLogFunction();
    UNUSED(clientId);
    MsgClients msgClients(inMessage);
    std::vector<uint64_t> clients;

    this->m_peerMutex.lock();

    for (auto &slot: this->m_broadcasts) {
        if (msgClients.clientType() == MsgClients::ClientType_Any
            && slot.second->broadcaster.pid
            && std::find(clients.begin(),
                         clients.end(),
                         slot.second->broadcaster.pid) == clients.end()) {
            clients.push_back(slot.second->broadcaster.pid);
        }

        for (auto &client: slot.second->listeners)
            if (std::find(clients.begin(),
                          clients.end(),
                          client.pid) == clients.end())
                clients.push_back(client.pid);
    }

    this->m_peerMutex.unlock();
    outMessage = MsgClients(msgClients.clientType(),
                            clients,
                            inMessage.queryId()).toMessage();

    return true;
}

bool AkVCam::ServicePrivate::broadcast(uint64_t clientId,
                                       const Message &inMessage,
                                       Message &outMessage)
{
    AkLogFunction();
    MsgBroadcast msgBroadcast(inMessage);
    MsgStatus status(-1, inMessage.queryId());

    // Take the slot (and claim broadcaster) under the map/metadata mutex only.
    this->m_peerMutex.lock();

    if (this->m_broadcasts.count(msgBroadcast.device()) < 1)
        this->m_broadcasts[msgBroadcast.device()] =
            std::make_shared<BroadcastSlot>();

    auto slot = this->m_broadcasts[msgBroadcast.device()];

    if (slot->broadcaster.pid == 0)
        slot->broadcaster = {clientId, msgBroadcast.pid()};

    bool isOwner = slot->broadcaster.pid == msgBroadcast.pid()
                   && slot->broadcaster.clientId == clientId;

    this->m_peerMutex.unlock();

    // Store the frame under the slot's OWN mutex, not the global one, so a
    // 3 MB frame store never blocks another camera's delivery.
    if (isOwner) {
        {
            std::lock_guard<std::mutex> frameLock(slot->frameMutex);
            slot->frame = msgBroadcast.frame();
        }
        slot->frameAvailable.notify_all();
        status = MsgStatus(0, inMessage.queryId());
    }

    outMessage = status.toMessage();

    return status.status() == 0;
}

bool AkVCam::ServicePrivate::listen(uint64_t clientId,
                                    const Message &inMessage,
                                    Message &outMessage)
{
    AkLogFunction();
    MsgListen msgListen(inMessage);
    bool ok = false;

    // Register the listener + take the slot under the map/metadata mutex only.
    this->m_peerMutex.lock();

    if (this->m_broadcasts.count(msgListen.device()) < 1)
        this->m_broadcasts[msgListen.device()] =
            std::make_shared<BroadcastSlot>();

    auto slot = this->m_broadcasts[msgListen.device()];
    slot->listeners.push_back({clientId, msgListen.pid()});
    bool active = slot->broadcaster.pid != 0;

    this->m_peerMutex.unlock();

    // Wait for and take the frame under the slot's OWN mutex + CV, and
    // serialize the response AFTER releasing it. Nothing here touches the
    // global m_peerMutex, so each camera's delivery is fully independent --
    // this is what lifts the ~5 concurrent-listener ceiling. The per-device
    // predicate also means only THIS device's broadcast wakes this listener.
    VideoFrame frame;
    {
        std::unique_lock<std::mutex> frameLock(slot->frameMutex);

        if (!slot->frame)
            slot->frameAvailable.wait_for(
                frameLock,
                std::chrono::seconds(1),
                [&slot] { return bool(slot->frame); });

        frame = std::move(slot->frame);
        slot->frame = {};
    }

    outMessage = MsgFrameReady(msgListen.device(),
                               frame,
                               active,
                               inMessage.queryId()).toMessage();
    ok = true;

    return ok;
}
