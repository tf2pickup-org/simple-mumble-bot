import { UserRemove, UserState } from '@proto/Mumble';
import { filter, map, tap } from 'rxjs';
import { Client } from './client';
import { MumbleSocket } from './mumble-socket';
import { User } from './user';

export class UserManager {
  private _users = new Map<number, User>();

  constructor(public readonly client: Client) {
    this.client.on('socketConnected', (socket: MumbleSocket) => {
      this._users.clear();

      socket.packet
        .pipe(
          filter(packet => packet.$type === UserState.$type),
          map(packet => packet as UserState),
          tap(userState => this.syncUser(userState)),
        )
        .subscribe();

      socket.packet
        .pipe(
          filter(packet => packet.$type === UserRemove.$type),
          map(packet => packet as UserRemove),
          tap(userRemove => this.removeUser(userRemove)),
        )
        .subscribe();
    });
  }

  bySession(session: number): User | undefined {
    return this._users.get(session);
  }

  private syncUser(userState: UserState) {
    let user = this.bySession(userState.session);
    if (!user) {
      user = new User(this.client, userState);
      this._users.set(user.session, user);
      this.client.emit('userCreate', user);
    } else {
      user.sync(userState);
    }
  }

  private removeUser(userRemove: UserRemove) {
    const user = this.bySession(userRemove.session);
    if (user) {
      this._users.delete(userRemove.session);
      this.client.emit('userRemove', user);
    }
  }
}
