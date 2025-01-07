import {
  ACL,
  ChannelRemove,
  ChannelState,
  PermissionQuery,
} from '@tf2pickup-org/mumble-protocol';
import { Change } from './change';
import { Client } from './client';
import { InsufficientPermissionsError, NoSuchChannelError } from './errors';
import { Permissions } from './permissions';
import { User } from './user';
import { syncProperty } from './sync-property';

type ChannelChangeableProps = Pick<
  Channel,
  'name' | 'parent' | 'temporary' | 'linkedChannels'
>;

export type ChannelChanges = {
  -readonly [P in keyof ChannelChangeableProps]?: Change<Channel[P]>;
};

/**
 * Represents a single channel.
 */
export class Channel {
  readonly id: number;
  name?: string;
  parent?: number;
  temporary: boolean;
  private links: number[] = [];

  constructor(
    public readonly client: Client,
    channelState: ChannelState & { channelId: number },
  ) {
    this.id = channelState.channelId;
    this.name = channelState.name;
    this.parent = channelState.parent;
    this.links = [...channelState.links];
    this.temporary = channelState.temporary ?? false;
  }

  /**
   * @internal
   */
  syncState(channelState: ChannelState): ChannelChanges {
    const changes: ChannelChanges = {
      ...syncProperty(this, 'name', channelState.name),
      ...syncProperty(this, 'parent', channelState.parent),
      ...syncProperty(this, 'temporary', channelState.temporary),
    };

    if (channelState.linksAdd.length + channelState.linksRemove.length > 0) {
      changes.linkedChannels = {
        previousValue: [...this.linkedChannels],
        currentValue: [],
      };

      this.links = [
        ...new Set([
          ...this.links,
          ...channelState.links,
          ...channelState.linksAdd,
        ]),
      ].filter(l => !channelState.linksRemove.includes(l));

      changes.linkedChannels.currentValue = this.linkedChannels;
    }

    return changes;
  }

  /**
   * List of users that are currently connected to the channel.
   */
  get users(): User[] {
    return this.client.users.findAll(user => user.channelId === this.id);
  }

  /**
   * List of sub-channels of this channel.
   */
  get subChannels(): Channel[] {
    return this.client.channels.findAll(channel => channel.parent === this.id);
  }

  /**
   * List of channels that are linked to this one.
   */
  get linkedChannels(): Channel[] {
    return this.links
      .map(ch => this.client.channels.byId(ch))
      .filter(ch => ch !== undefined) as Channel[];
  }

  /**
   * Create a sub-channel.
   * @param name The name of the channel to create.
   * @returns The newly created channel.
   */
  async createSubChannel(name: string): Promise<Channel> {
    const permissions = await this.getPermissions();
    if (!permissions.canCreateChannel) {
      throw new InsufficientPermissionsError();
    }

    const ret = await this.client.command('createChannel', {
      sendPacket: [
        ChannelState,
        ChannelState.create({ parent: this.id, name }),
      ],
      expectPacket: [
        ChannelState,
        channelState =>
          channelState.parent === this.id && channelState.name === name,
      ],
    });
    return this.client.channels.byId(ret.channelId!)!;
  }

  /**
   * Remove this channel and all sub-channels.
   * @returns This channel.
   */
  async remove(): Promise<this> {
    const permissions = await this.getPermissions();
    if (!permissions.canRemoveChannel) {
      throw new InsufficientPermissionsError();
    }

    await this.client.command('removeChannel', {
      sendPacket: [ChannelRemove, ChannelRemove.create({ channelId: this.id })],
      expectPacket: [ChannelRemove, ({ channelId }) => channelId === this.id],
    });
    return this;
  }

  /**
   * Fetch permissions to this channel.
   * @returns Permissions.
   */
  async getPermissions(): Promise<Permissions> {
    if (this.client.user?.userId === 0) {
      return Permissions.superUser();
    }

    if (this.client.permissions.has(this.id)) {
      return this.client.permissions.get(this.id)!;
    }

    const ret = await this.client.command('fetchChannelPermissions', {
      sendPacket: [
        PermissionQuery,
        PermissionQuery.create({ channelId: this.id }),
      ],
      expectPacket: [PermissionQuery, ({ channelId }) => channelId === this.id],
    });
    return new Permissions(ret.permissions ?? 0);
  }

  /**
   * Link two channels together.
   * @param otherChannel The other channel to link to this one.
   * @returns This channel.
   */
  async link(otherChannel: Channel | number): Promise<this> {
    if (!(await this.getPermissions()).canLinkChannel) {
      throw new InsufficientPermissionsError();
    }

    const targetChannel =
      typeof otherChannel === 'number'
        ? this.client.channels.byId(otherChannel)
        : otherChannel;
    if (targetChannel === undefined) {
      throw new NoSuchChannelError(
        typeof otherChannel === 'number' ? otherChannel : otherChannel.id,
      );
    }

    if (!(await targetChannel.getPermissions()).canLinkChannel) {
      throw new InsufficientPermissionsError();
    }

    await this.client.command('linkChannels', {
      sendPacket: [
        ChannelState,
        ChannelState.create({
          channelId: this.id,
          linksAdd: [targetChannel.id],
        }),
      ],
      expectPacket: [ChannelState, ({ channelId }) => channelId === this.id],
    });
    return this;
  }

  /**
   * Removes link between two channels.
   * @param otherChannel The channel to unlink.
   * @returns This channel.
   */
  async unlink(otherChannel: Channel | number): Promise<this> {
    if (!(await this.getPermissions()).canLinkChannel) {
      throw new InsufficientPermissionsError();
    }

    const targetChannel =
      typeof otherChannel === 'number'
        ? this.client.channels.byId(otherChannel)
        : otherChannel;
    if (targetChannel === undefined) {
      throw new NoSuchChannelError(
        typeof otherChannel === 'number' ? otherChannel : otherChannel.id,
      );
    }

    if (!(await targetChannel.getPermissions()).canLinkChannel) {
      throw new InsufficientPermissionsError();
    }

    await this.client.command('unlinkChannels', {
      sendPacket: [
        ChannelState,
        ChannelState.create({
          channelId: this.id,
          linksRemove: [targetChannel.id],
        }),
      ],
      expectPacket: [ChannelState, ({ channelId }) => channelId === this.id],
    });
    return this;
  }

  async fetchAcl(): Promise<ACL> {
    const acl = await this.client.command('fetchAcl', {
      sendPacket: [
        ACL,
        ACL.create({
          channelId: this.id,
          query: true,
        }),
      ],
      expectPacket: [ACL, ({ channelId }) => channelId === this.id],
    });
    return acl;
  }

  async saveAcl(acl: ACL) {
    this.client.command('saveAcl', {
      sendPackets: [
        [ACL, ACL.create({ ...acl, channelId: this.id })],
        [
          ACL,
          ACL.create({
            channelId: this.id,
            query: true,
          }),
        ],
      ],
      expectPacket: [ACL, ({ channelId }) => channelId === this.id],
    });
  }
}
