import {Injectable} from '@angular/core';
import {Observable} from 'rxjs';
import {Topics} from './topics';
import {HttpClient} from '@angular/common/http';
import {environment} from '../../environments/environment';
import {Backend} from '../app.backend';
import {TopicsBackendService} from './topics.backend.service';
import {TopicsDemoService} from './topics.demo.service';

@Injectable({
  providedIn: 'root'
})
export class TopicsService {

  constructor() {
  }

  getTopics(serverId: string): Observable<Topics> {
    return null;
  }
}

export function topicsServiceFactory(http: HttpClient): TopicsService {
  switch (environment.backend) {
    case Backend.SERVER: {
      return new TopicsBackendService(http);
    }
    case Backend.DEMO: {
      return new TopicsDemoService();
    }
  }
}
