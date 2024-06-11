import type { BaileysEventEmitter, proto } from '@whiskeysockets/baileys';
import { PrismaClientKnownRequestError } from '@prisma/client/runtime';
import { useLogger, usePrisma } from '../shared';
import type { BaileysEventHandler } from '../types';
import { transformPrisma } from '../utils';

export default function chatHandler(sessionId: string, event: BaileysEventEmitter) {
  const prisma = usePrisma();
  const logger = useLogger();
  let listening = false;

  const set: BaileysEventHandler<'messaging-history.set'> = async ({ chats, isLatest }) => {
    try {
      await prisma.$transaction(
        async (tx: {
          chat: {
            deleteMany: (arg0: { where: { sessionId: string } }) => any;
            findMany: (arg0: {
              select: { id: boolean };
              where: { id: { in: string[] }; sessionId: string };
            }) => any;
            createMany: (arg0: {
              data: {
                sessionId: string;
                id: string;
                messages?: object | undefined;
                newJid?: string | undefined;
                oldJid?: string | undefined;
                lastMsgTimestamp?: number | object | undefined;
                unreadCount?: number | undefined;
                readOnly?: boolean | undefined;
                endOfHistoryTransfer?: boolean | undefined;
                ephemeralExpiration?: number | undefined;
                ephemeralSettingTimestamp?: number | object | undefined;
                endOfHistoryTransferType?: proto.Conversation.EndOfHistoryTransferType | undefined;
                conversationTimestamp?: number | object | undefined;
                name?: string | undefined;
                pHash?: string | undefined;
                notSpam?: boolean | undefined;
                archived?: boolean | undefined;
                disappearingMode?: object | undefined;
                unreadMentionCount?: number | undefined;
                markedAsUnread?: boolean | undefined;
                participant?: object | undefined;
                tcToken?: Buffer | undefined;
                tcTokenTimestamp?: number | object | undefined;
                contactPrimaryIdentityKey?: Buffer | undefined;
                pinned?: number | undefined;
                muteEndTime?: number | object | undefined;
                wallpaper?: object | undefined;
                mediaVisibility?: proto.MediaVisibility | undefined;
                tcTokenSenderTimestamp?: number | object | undefined;
                suspended?: boolean | undefined;
                terminated?: boolean | undefined;
                createdAt?: number | object | undefined;
                createdBy?: string | undefined;
                description?: string | undefined;
                support?: boolean | undefined;
                isParentGroup?: boolean | undefined;
                parentGroupId?: string | undefined;
                isDefaultSubgroup?: boolean | undefined;
                displayName?: string | undefined;
                pnJid?: string | undefined;
                shareOwnPn?: boolean | undefined;
                pnhDuplicateLidThread?: boolean | undefined;
                lidJid?: string | undefined;
                username?: string | undefined;
                lidOriginType?: string | undefined;
                commentsCount?: number | undefined;
                lastMessageRecvTimestamp?: number | undefined;
              }[];
            }) => any;
          };
        }) => {
          if (isLatest) await tx.chat.deleteMany({ where: { sessionId } });

          const existingIds = (
            await tx.chat.findMany({
              select: { id: true },
              where: { id: { in: chats.map((c) => c.id) }, sessionId },
            })
          ).map((i: { id: any }) => i.id);
          const chatsAdded = (
            await tx.chat.createMany({
              data: chats
                .filter((c) => !existingIds.includes(c.id))
                .map((c) => ({ ...transformPrisma(c), sessionId })),
            })
          ).count;

          logger.info({ chatsAdded }, 'Synced chats');
        }
      );
    } catch (e) {
      logger.error(e, 'An error occured during chats set');
    }
  };

  const upsert: BaileysEventHandler<'chats.upsert'> = async (chats) => {
    try {
      await Promise.any(
        chats
          .map((c) => transformPrisma(c))
          .map((data) =>
            prisma.chat.upsert({
              select: { pkId: true },
              create: { ...data, sessionId },
              update: data,
              where: { sessionId_id: { id: data.id, sessionId } },
            })
          )
      );
    } catch (e) {
      logger.error(e, 'An error occured during chats upsert');
    }
  };

  const update: BaileysEventHandler<'chats.update'> = async (updates) => {
    for (const update of updates) {
      try {
        const data = transformPrisma(update);
        await prisma.chat.update({
          select: { pkId: true },
          data: {
            ...data,
            unreadCount:
              typeof data.unreadCount === 'number'
                ? data.unreadCount > 0
                  ? { increment: data.unreadCount }
                  : { set: data.unreadCount }
                : undefined,
          },
          where: { sessionId_id: { id: update.id!, sessionId } },
        });
      } catch (e) {
        if (e instanceof PrismaClientKnownRequestError && e.code === 'P2025') {
          return logger.info({ update }, 'Got update for non existent chat');
        }
        logger.error(e, 'An error occured during chat update');
      }
    }
  };

  const del: BaileysEventHandler<'chats.delete'> = async (ids) => {
    try {
      await prisma.chat.deleteMany({
        where: { id: { in: ids } },
      });
    } catch (e) {
      logger.error(e, 'An error occured during chats delete');
    }
  };

  const listen = () => {
    if (listening) return;

    event.on('messaging-history.set', set);
    event.on('chats.upsert', upsert);
    event.on('chats.update', update);
    event.on('chats.delete', del);
    listening = true;
  };

  const unlisten = () => {
    if (!listening) return;

    event.off('messaging-history.set', set);
    event.off('chats.upsert', upsert);
    event.off('chats.update', update);
    event.off('chats.delete', del);
    listening = false;
  };

  return { listen, unlisten };
}
