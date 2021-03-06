import { Injectable } from '@angular/core';
import { Subject ,  Observable } from 'rxjs';

@Injectable()
export class SearchService {

  private phrase = new Subject<string>();
  private currentPhrase;

  setState(state: string) {
    this.phrase.next(state);
    this.currentPhrase = state;
  }

  getState(): Observable<string> {
    return this.phrase.asObservable();
  }

  getCurrentPhrase() {
    return this.currentPhrase;
  }

}
