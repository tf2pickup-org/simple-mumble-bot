import { tlsConnect } from './tls-connect';
import { MumbleSocket } from './mumble-socket';
import {
  concatMap,
  exhaustMap,
  filter,
  interval,
  lastValueFrom,
  map,
  race,
  switchMap,
  take,
  tap,
  throwError,
  timeout,
  zip,
} from 'rxjs';
import {
  Authenticate,
  PermissionDenied,
  PermissionQuery,
  Ping,
  Reject,
  ServerConfig,
  ServerSync,
  UserList,
  UserList_User,
  UserRemove,
  Version,
} from '@tf2pickup-org/mumble-protocol';
import { User } from './user';
import { ChannelManager } from './channel-manager';
import { UserManager } from './user-manager';
import { encodeMumbleVersion } from './encode-mumble-version';
import { ClientOptions } from './client-options';
import {
  ClientDisconnectedError,
  CommandTimedOutError,
  ConnectionRejectedError,
  PermissionDeniedError,
} from './errors';
import { filterPacket } from './rxjs-operators/filter-packet';
import { platform, release } from 'os';
import { Permissions } from './permissions';
import { encodeMumbleVersionLegacy } from './encode-mumble-version-legacy';
import { TypedEventEmitter } from './typed-event-emitter';
import { Events } from './events';
import { MessageType } from '@protobuf-ts/runtime';
import { CommandTimeout } from './config';

type PacketTuple<T extends object> = [MessageType<T>, T];
type ManyPackets<T extends object[]> = { [K in keyof T]: PacketTuple<T[K]> };

type CommandProps<
  Send extends object,
  Return extends object,
  SendMore extends object[] = [],
> = { expectPacket: [MessageType<Return>, (packet: Return) => boolean] } & (
  | {
      sendPacket: PacketTuple<Send>;
    }
  | {
      sendPackets: ManyPackets<[Send, ...SendMore]>;
    }
);

const defaultOptions: Partial<ClientOptions> = {
  port: 64738,
  clientName: 'simple mumble bot',
  pingInterval: 10000,
};

interface ConnectedClient extends Client {
  socket: MumbleSocket;
  session: number;
  user: User;
}

export class Client extends TypedEventEmitter<Events, Events> {
  channels: ChannelManager = new ChannelManager(this);
  users: UserManager = new UserManager(this);
  serverVersion?: Version;
  serverConfig?: ServerConfig;
  socket?: MumbleSocket;
  session?: number;
  welcomeText?: string;
  readonly options: ClientOptions;

  // Channel permission cache
  readonly permissions = new Map<number, Permissions>();

  constructor(options: ClientOptions) {
    super();
    this.options = { ...defaultOptions, ...options };
  }

  /**
   * The currently logged-in user.
   */
  get user() {
    if (!this.session) {
      return undefined;
    }

    return this.users.bySession(this.session);
  }

  /**
   * @returns true if the client is connected; false otherwise.
   */
  isConnected(): this is ConnectedClient {
    return !!this.socket;
  }

  /**
   * @throws ClientDisconnectedError if the client is not connected.
   */
  assertConnected(): asserts this is ConnectedClient {
    if (!this.socket) {
      throw new ClientDisconnectedError();
    }
  }

  /**
   * Establishes a connection to the provided Mumble server.
   */
  async connect(): Promise<this> {
    this.socket = new MumbleSocket(
      await tlsConnect(this.options.host, this.options.port, this.options),
    );
    this.emit('socketConnect', this.socket);

    this.socket.packet
      .pipe(
        filterPacket(PermissionQuery),
        filter(permissionQuery => permissionQuery.channelId !== undefined),
        map(permissionQuery => ({
          channelId: permissionQuery.channelId!,
          permissions: new Permissions(permissionQuery.permissions ?? 0),
        })),
      )
      .subscribe(({ channelId, permissions }) => {
        this.permissions.set(channelId, permissions);
      });

    this.socket.packet
      .pipe(
        filterPacket(UserRemove),
        filter(userRemove => userRemove.session === this.session),
      )
      .subscribe(userRemove => {
        this.emit('disconnect', {
          reason: userRemove.reason,
        });
        this.socket = undefined;
      });

    const initialize: Promise<this> = lastValueFrom(
      race(
        zip(
          this.socket.packet.pipe(filterPacket(ServerSync), take(1)),
          this.socket.packet.pipe(filterPacket(ServerConfig), take(1)),
          this.socket.packet.pipe(filterPacket(Version), take(1)),
        ).pipe(
          tap(([serverSync, serverConfig, version]) => {
            if (serverSync.session) {
              this.session = serverSync.session;
            }
            this.welcomeText = serverSync.welcomeText;
            this.serverVersion = version;
            this.serverConfig = serverConfig;
            this.emit('connect');
            this.startPinger();
          }),
          map(() => this),
        ),
        this.socket.packet.pipe(
          filterPacket(Reject),
          switchMap(reject =>
            throwError(() => new ConnectionRejectedError(reject)),
          ),
        ),
      ),
    );

    await this.authenticate();
    await this.sendVersion();
    await this.ping();
    return await initialize;
  }

  /**
   * Disconnects from the server.
   */
  disconnect(): this {
    this.emit('disconnect');
    this.socket?.end();
    this.socket = undefined;
    return this;
  }

  /**
   * Wraps sending and receiving a packet in a convenient method.
   * Handles denied permissions response as well as command timeout.
   */
  async command<
    Send extends object,
    Return extends object,
    SendMore extends object[] = [],
  >(name: string, props: CommandProps<Send, Return, SendMore>) {
    this.assertConnected();
    const ret = lastValueFrom(
      race(
        this.socket.packet.pipe(
          filterPacket(props.expectPacket[0]),
          filter(props.expectPacket[1]),
          take(1),
          timeout({
            first: CommandTimeout,
            with: () => throwError(() => new CommandTimedOutError(name)),
          }),
        ),
        this.socket.packet.pipe(
          filterPacket(PermissionDenied),
          filter(pd => pd.session === this.session),
          take(1),
          concatMap(pd => throwError(() => new PermissionDeniedError(pd))),
        ),
      ),
    );

    if ('sendPackets' in props) {
      for (const packet of props.sendPackets) {
        await this.socket.send(packet[0], packet[1]);
      }
    } else {
      await this.socket.send(props.sendPacket[0], props.sendPacket[1]);
    }
    return ret;
  }

  /**
   * Fetch the list of users registered with the server.
   */
  async fetchRegisteredUsers(): Promise<UserList_User[]> {
    const ret = await this.command('fetchRegisteredUsers', {
      sendPacket: [UserList, UserList.create()],
      expectPacket: [UserList, () => true],
    });
    return ret.users;
  }

  /**
   * Deregisters the user; the user does not have to be online.
   */
  async deregisterUser(userId: number): Promise<void> {
    this.assertConnected();
    this.command('deregisterUser', {
      sendPackets: [
        [UserList, UserList.create({ users: [{ userId, name: undefined }] })],
        [UserList, UserList.create()],
      ],
      expectPacket: [UserList, () => true],
    });
  }

  /**
   * Rename registered user; the user does not have to be online.
   */
  async renameRegisteredUser(userId: number, name: string): Promise<void> {
    this.assertConnected();
    this.command('renameRegisteredUser', {
      sendPackets: [
        [UserList, UserList.create({ users: [{ userId, name }] })],
        // we need to send another packet, so we can wait on the expected UserList packet
        // we cannot just listen for a UserState packet, as the user we are renaming
        // may be offline
        // unfortunately, this re-sends the entire list
        [UserList, UserList.create()],
      ],
      expectPacket: [UserList, () => true],
    });
  }

  /**
   * @internal
   */
  private async sendVersion(): Promise<void> {
    const version = {
      major: 1,
      minor: 4,
      patch: 287,
    };
    return await this.socket?.send(
      Version,
      Version.create({
        release: this.options.clientName,
        versionV1: encodeMumbleVersionLegacy(version),
        versionV2: encodeMumbleVersion(version),
        os: platform(),
        osVersion: release(),
      }),
    );
  }

  /**
   * @internal
   */
  private async authenticate(): Promise<void> {
    return await this.socket?.send(
      Authenticate,
      Authenticate.create({
        username: this.options.username,
        password: this.options.password,
        tokens: this.options.tokens,
        opus: true,
      }),
    );
  }

  /**
   * @internal
   */
  private async ping() {
    return await this.socket?.send(Ping, Ping.create());
  }

  /**
   * @internal
   */
  private startPinger() {
    const subscription = interval(this.options.pingInterval)
      .pipe(exhaustMap(() => this.ping()))
      .subscribe();
    this.on('disconnect', () => subscription.unsubscribe());
  }
}
