import { CommandTimeout } from '@/config';
import { CommandTimedOutError, PermissionDeniedError } from '@/errors';
import { MumbleSocket } from '@/mumble-socket';
import { filterPacket } from '@/rxjs-operators/filter-packet';
import {
  ChannelRemove,
  PermissionDenied,
} from '@tf2pickup-org/mumble-protocol';
import {
  concatMap,
  filter,
  lastValueFrom,
  map,
  race,
  take,
  throwError,
  timer,
} from 'rxjs';

export const removeChannel = async (
  socket: MumbleSocket,
  channelId: number,
): Promise<number> => {
  const ret = lastValueFrom(
    race(
      socket.packet.pipe(
        filterPacket(ChannelRemove),
        filter(channelRemove => channelRemove.channelId === channelId),
        take(1),
        map(channelRemove => channelRemove.channelId),
      ),
      socket.packet.pipe(
        filterPacket(PermissionDenied),
        filter(permissionDenied => permissionDenied.channelId === channelId),
        take(1),
        concatMap(permissionDenied =>
          throwError(() => new PermissionDeniedError(permissionDenied)),
        ),
      ),
      timer(CommandTimeout).pipe(
        concatMap(() =>
          throwError(() => new CommandTimedOutError('removeChannel')),
        ),
      ),
    ),
  );
  socket.send(ChannelRemove, ChannelRemove.create({ channelId }));
  return ret;
};
