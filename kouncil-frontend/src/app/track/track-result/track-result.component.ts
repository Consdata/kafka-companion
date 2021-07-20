import {Component, OnDestroy, OnInit, ViewChild} from '@angular/core';
import {Subscription} from 'rxjs';
import {ActivatedRoute} from '@angular/router';
import {SearchService} from '../../search.service';
import {Title} from '@angular/platform-browser';
import {ProgressBarService} from '../../util/progress-bar.service';
import {DrawerService} from '../../util/drawer.service';
import {ServersService} from '../../servers.service';
import {MessageViewComponent} from '../../topic/message/message-view.component';
import {TrackService} from '../track.service';
import {TrackFilter} from '../track-filter/track-filter';
import {RxStompService} from '@stomp/ng2-stompjs';
import {Crypto} from '../../util/crypto';

@Component({
  selector: 'app-track-result',
  template: `
    <div class="track">
      <ng-template #noDataPlaceholder>
        <app-no-data-placeholder
          [objectTypeName]="'Message'"></app-no-data-placeholder>
      </ng-template>
      <ngx-datatable
        *ngIf="filteredRows && filteredRows.length > 0; else noDataPlaceholder"
        class="track-table material expandable"
        [rows]="filteredRows"
        [rowHeight]="48"
        [headerHeight]="48"
        [footerHeight]="80"
        [scrollbarH]="true"
        [scrollbarV]="true"
        [columnMode]="'force'"
        [loadingIndicator]="isLoading()"
        (activate)="showMessage($event)"
        #table>
        <ngx-datatable-column prop="timestamp" name="timestamp"
                              [width]="190">
          <ng-template let-value="value"
                       ngx-datatable-cell-template>
            {{value | date:'yyyy-MM-dd HH:mm:ss.SSS'}}
          </ng-template>
        </ngx-datatable-column>
        <ngx-datatable-column prop="topic" name="topic" [width]="190">
          <ng-template let-value="value"
                       ngx-datatable-cell-template>
            {{value}}
          </ng-template>
        </ngx-datatable-column>
        <ngx-datatable-column prop="partition" name="partition"
                              [width]="190">
          <ng-template let-value="value"
                       ngx-datatable-cell-template>
            {{value}}
          </ng-template>
        </ngx-datatable-column>
        <ngx-datatable-column prop="offset" name="offset" [width]="190">
          <ng-template let-value="value"
                       ngx-datatable-cell-template>
            {{value}}
          </ng-template>
        </ngx-datatable-column>
        <ngx-datatable-column prop="key" name="key" [width]="190">
          <ng-template let-value="value"
                       ngx-datatable-cell-template>
            {{value}}
          </ng-template>
        </ngx-datatable-column>
      </ngx-datatable>
    </div>
  `,
  styleUrls: ['./track-result.component.scss']
})
export class TrackResultComponent implements OnInit, OnDestroy {

  @ViewChild('table') table: any;
  searchSubscription: Subscription;
  trackFilterSubscription: Subscription;
  topicSubscription: Subscription;
  filteredRows = [];
  allRows = [];
  phrase: string;
  asyncHandle: string;

  constructor(private route: ActivatedRoute,
              private searchService: SearchService,
              private titleService: Title,
              private progressBarService: ProgressBarService,
              private trackService: TrackService,
              private drawerService: DrawerService,
              private servers: ServersService,
              private rxStompService: RxStompService) {
  }

  private static tryParseJson(message): string {
    try {
      return JSON.parse(message);
    } catch (e) {
      return message;
    }
  }

  private static getDestination(asyncHandle: string): string {
    return '/topic/track/' + asyncHandle;
  }

  ngOnInit(): void {
    this.searchSubscription = this.searchService.getState().subscribe(
      phrase => {
        this.phrase = phrase;
        this.filterRows();
      });
    this.trackFilterSubscription = this.trackService.trackFilterChange$.subscribe(trackFilter => {
      this.getEvents(trackFilter);
    });
    if (this.trackService.isAsyncEnable()) {
      this.asyncHandle = Crypto.uuidv4();
      this.topicSubscription = this.rxStompService.watch(TrackResultComponent.getDestination(this.asyncHandle))
        .subscribe((message) => {
          this.onMessageReceived(message);
        });
    }
  }

  ngOnDestroy() {
    this.searchSubscription.unsubscribe();
    this.trackFilterSubscription.unsubscribe();
    this.topicSubscription.unsubscribe();
  }

  onMessageReceived(message) {
    this.filteredRows = [];
    setTimeout(() => {
      const items = JSON.parse(message.body);
      this.allRows = [...this.allRows, ...items];
      this.filterRows();
    });
  }

  showMessage(event): void {
    if (event.type === 'click') {
      this.drawerService.openDrawerWithPadding(MessageViewComponent, {
        source: TrackResultComponent.tryParseJson(event.row.value),
        headers: event.row.headers,
        key: event.row.key,
        topicName: event.row.topic
      });
    }
  }

  isLoading(): boolean {
    return this.progressBarService.progressSub.getValue();
  }

  private filterRows() {
    this.filteredRows = this.allRows.filter((row) => {
      return !this.phrase || JSON.stringify(row).toLowerCase().indexOf(this.phrase.toLowerCase()) > -1;
    });
  }

  private getEvents(trackFilter: TrackFilter) {
    this.filteredRows = [];
    this.allRows = [];
    setTimeout(() => {
      this.trackService.getEvents(this.servers.getSelectedServerId(), trackFilter, this.asyncHandle).subscribe(events => {
        if (events && events.length > 0) {
          this.allRows = [...this.allRows, ...events];
          this.filterRows();
        }
      });
    });
  }

}
