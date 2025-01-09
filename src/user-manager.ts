import { UserRemove, UserState } from '@tf2pickup-org/mumble-protocol';
import { tap } from 'rxjs';
import { Client } from './client';
import { filterPacket } from './rxjs-operators/filter-packet';
import { MumbleSocket } from './mumble-socket';
import { User } from './user';

/**
 * A manager of users.
 */
export class UserManager {
  private _users = new Map<number, User>();

  constructor(public readonly client: Client) {
    this.client.on('socketConnect', (socket: MumbleSocket) => {
      this._users.clear();

      socket.packet
        .pipe(
          filterPacket(UserState),
          tap(userState => this.syncUser(userState)),
        )
        .subscribe();

      socket.packet
        .pipe(
          filterPacket(UserRemove),
          tap(userRemove => this.removeUser(userRemove)),
        )
        .subscribe();
    });
  }

  /**
   * Finds a user by their session id.
   * @param session The user's session.
   * @returns User (if found) or undefined.
   */
  bySession(session: number): User | undefined {
    return this._users.get(session);
  }

  /**
   * Finds a user by their name.
   * @param name The name of the user to look for.
   * @returns User (if found) or undefined.
   */
  byName(name: string): User | undefined {
    return Array.from(this._users.values()).find(u => u.name === name);
  }

  findAll(predicate: (user: User) => boolean): User[] {
    return Array.from(this._users.values()).filter(predicate);
  }

  /**
   * @internal
   */
  private syncUser(userState: UserState) {
    if (userState.session === undefined) {
      return;
    }

    let user = this.bySession(userState.session);
    if (!user) {
      user = new User(
        this.client,
        userState as UserState & { session: number },
      );
      this._users.set(user.session, user);
      /**
       * Emitted whenever a user is added to the client's list of users.
       * @event Client#userCreate
       * @property {User} user The user that was created.
       */
      this.client.emit('userCreate', user);
    } else {
      const changes = user.syncState(userState);
      if (Object.keys(changes).length > 0) {
        /**
         * Emitted when an user is updated.
         * @event Client#userUpdate
         * @property {User} user The user that was updated.
         * @property {UserChanges} changes What changes were made to the user.
         */
        this.client.emit('userUpdate', user, changes);
      }
    }
  }

  /**
   * @internal
   */
  private removeUser(userRemove: UserRemove) {
    const user = this.bySession(userRemove.session);
    if (user) {
      this._users.delete(userRemove.session);
      /**
       * Emitted whenever a user is disconnected from the server.
       * @event Client#userRemove
       * @property {User} user The user that was removed.
       */
      this.client.emit('userRemove', user);
    }
  }
}
