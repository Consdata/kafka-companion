import {Injectable} from '@angular/core';
import {Observable} from 'rxjs';
import {Message} from '../topic/message';
import {HttpClient} from '@angular/common/http';
import {environment} from '../../environments/environment';
import {Backend} from '../app.backend';
import {SendBackendService} from './send.backend.service';
import {SendDemoService} from './send.demo.service';

@Injectable({
  providedIn: 'root'
})
export class SendService {

  constructor() {
  }

  send(serverId: string, topic: string, count: number, message: Message): Observable<Object> {
    return undefined;
  }

}

export function sendServiceFactory(http: HttpClient): SendService {
  switch (environment.backend) {
    case Backend.SERVER: {
      return new SendBackendService(http);
    }
    case Backend.DEMO: {
      return new SendDemoService();
    }
  }
}
