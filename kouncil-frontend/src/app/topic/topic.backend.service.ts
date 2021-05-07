import {Injectable} from '@angular/core';
import {TopicService} from './topic.service';
import {BehaviorSubject, Observable, Subject} from 'rxjs';
import {TopicMessages} from './topic';
import {Page} from './page';
import {HttpClient} from '@angular/common/http';
import {ProgressBarService} from '../util/progress-bar.service';

@Injectable({
  providedIn: 'root'
})
export class TopicBackendService implements TopicService {

  readonly VISIBLE_PARTITION_QUANTITY: number = 10;

  partitionOffsets: { [key: number]: number } = {};
  partitionEndOffsets: { [key: number]: number } = {};
  partitions: number[];
  selectedPartitions: number[];
  visiblePartitions: number[];
  convertTopicMessagesJsonToGrid$: Subject<TopicMessages> = new Subject<TopicMessages>();
  selectedPartitionsChanged$: Subject<number[]> = new Subject<number[]>();
  visiblePartitionsChanged$: Subject<number[]> = new Subject<number[]>();
  onePartitionSelected$: BehaviorSubject<boolean> = new BehaviorSubject<boolean>(false);
  paginationChanged$: BehaviorSubject<Page>;

  constructor(public http: HttpClient, public progressBarService: ProgressBarService) {
    this.initPaging();
  }

  getMessages(topicName: string) {
    let url;
    if (typeof this.selectedPartitions !== 'undefined') {
      let partitionsParam = '';
      for (let i = 0; i < this.selectedPartitions.length; i++) {
        if (this.selectedPartitions[i] === 1) {
          partitionsParam += i + ',';
        }
      }
      if (partitionsParam === '') {
        return;
      }
      url = `/api/topic/messages/${topicName}/${partitionsParam}/latest`;
    } else {
      url = `/api/topic/messages/${topicName}/all/latest`;
    }
    url += this.addPagingToUrl();
    this.http.get(url).subscribe((data: TopicMessages) => {
      this.processMessagesData(data);
    });
    this.onePartitionSelected();
  }

  processMessagesData(data: TopicMessages): void {
    this.partitionOffsets = data.partitionOffsets;
    this.partitionEndOffsets = data.partitionEndOffsets;
    const paging = this.paginationChanged$.getValue();
    paging.totalElements = data.totalResults;
    this.convertTopicMessagesJsonToGrid$.next(data);
    this.progressBarService.setProgress(false);
    this.partitions = Array.from({length: Object.values(this.partitionOffsets).length}, (v, i) => i);
    if (typeof this.selectedPartitions === 'undefined') {
      this.selectedPartitions = Array.from({length: Object.values(this.partitionOffsets).length}, () => 1);
      this.selectedPartitionsChanged$.next(this.selectedPartitions);
      this.onePartitionSelected();
    }
    if (typeof this.visiblePartitions === 'undefined') {
      this.visiblePartitions = this.partitions.slice(0, this.VISIBLE_PARTITION_QUANTITY);
      this.visiblePartitionsChanged$.next(this.visiblePartitions);
    }
  }

  togglePartition(nr: any, topicName: string) {
    const index = this.partitions.findIndex(e => e === nr);
    this.selectedPartitions[index] = -1 * this.selectedPartitions[index];
    this.progressBarService.setProgress(true);
    this.getMessages(topicName);
  }

  previous() {
    if (this.partitions.length > this.VISIBLE_PARTITION_QUANTITY) {
      const index = this.getFirstElementIndex();
      const subPartitions = this.partitions.slice(index - 1, index + (this.VISIBLE_PARTITION_QUANTITY - 1));
      this.updateVisiblePartitions(subPartitions);
    }
  }

  next() {
    const index = this.getFirstElementIndex();
    const subPartitions = this.partitions.slice(index + 1, index + (this.VISIBLE_PARTITION_QUANTITY + 1));
    this.updateVisiblePartitions(subPartitions);
  }

  private updateVisiblePartitions(subPartitions: number[]) {
    if (subPartitions.length === this.VISIBLE_PARTITION_QUANTITY) {
      this.visiblePartitions = subPartitions;
      this.visiblePartitionsChanged$.next(this.visiblePartitions);
    }
  }

  private addPagingToUrl(): string {
    const paging = this.paginationChanged$.getValue();
    return `?offset=${(paging.pageNumber - 1) * paging.size}&limit=${paging.size}`;
  }

  private getFirstElementIndex(): number {
    const firstElement = this.visiblePartitions[0];
    return this.partitions.findIndex(e => e === firstElement);
  }

  onePartitionSelected(): void {
    this.onePartitionSelected$.next(
      this.selectedPartitions
      && this.selectedPartitions.filter(((value, index) => value === 1)).length === 1
    );
  }

  hasNoMorePrevValues(): boolean {
    return this.visiblePartitions[0] === this.partitions[0];
  }

  hasNoMoreNextValues(): boolean {
    return this.visiblePartitions[this.visiblePartitions.length - 1] === this.partitions[this.partitions.length - 1];
  }

  getConvertTopicMessagesJsonToGridObservable(): Observable<TopicMessages> {
    return this.convertTopicMessagesJsonToGrid$.asObservable();
  }

  getPartitionOffset(partitionNr: number): string {
    return this.partitionOffsets[partitionNr] + ' - ' + this.partitionEndOffsets[partitionNr];
  }

  showMorePartitions(): boolean {
    return this.partitions?.length > this.VISIBLE_PARTITION_QUANTITY;
  }

  getSelectedPartitionsObservable(): Observable<number[]> {
    return this.selectedPartitionsChanged$.asObservable();
  }

  getVisiblePartitionsObservable(): Observable<number[]> {
    return this.visiblePartitionsChanged$.asObservable();
  }

  isOnePartitionSelected$(): Observable<boolean> {
    return this.onePartitionSelected$.asObservable();
  }

  paginateMessages(event: any, topicName: string) {
    const paging = this.paginationChanged$.getValue();
    paging.pageNumber = event.page;
    this.paginationChanged$ = new BehaviorSubject<Page>(paging);
    this.getMessages(topicName);
  }

  initPaging(): void {
    const paging = new Page();
    paging.pageNumber = 1;
    paging.size = 20;
    this.paginationChanged$ = new BehaviorSubject<Page>(paging);
  }

  getPagination$(): Observable<Page> {
    return this.paginationChanged$.asObservable();
  }
}
