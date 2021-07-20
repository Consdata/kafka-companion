import { InjectableRxStompConfig } from '@stomp/ng2-stompjs';
import {environment} from '../environments/environment';

export const RxStompConfig: InjectableRxStompConfig = {
  brokerURL: `ws://${environment.websocketUrl}/ws`,
  heartbeatIncoming: 0,
  heartbeatOutgoing: 20000,
  reconnectDelay: 2000,
  debug: (msg: string): void => {
        console.log(msg);
  },
};
