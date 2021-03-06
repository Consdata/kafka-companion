import {Component, Input} from '@angular/core';
import {TopicService} from './topic.service';
import {MatSelectChange} from '@angular/material/select';
import {ServersService} from '../servers.service';

@Component({
  selector: 'topic-partitions',
  template: `
    <mat-form-field>
      <mat-select class="select" [(value)]="selectedPartition" (selectionChange)="togglePartition($event)">
        <mat-option value="all">All partitions</mat-option>
        <mat-option *ngFor="let i of partitions" value="{{i}}">{{i}}</mat-option>
      </mat-select>
    </mat-form-field>
  `,
  styleUrls: ['./topic-partitions.component.scss']
})
export class TopicPartitionsComponent {

  private ALL_PARTITIONS = 'all';

  @Input() topicName: string;

  selectedPartition = this.ALL_PARTITIONS;

  partitions = [];

  constructor(private topicService: TopicService, private servers: ServersService) {
    this.topicService.getNumberOfPartitionsObservable().subscribe(value => {
      this.partitions = Array.from(Array(value).keys());
    });
  }

  togglePartition(partition: MatSelectChange): void {
    const value = partition.value;
    this.selectedPartition = value;
    if (value === this.ALL_PARTITIONS) {
      this.topicService.selectAllPartitions(this.servers.getSelectedServerId(), this.topicName);
    } else {
      this.topicService.selectPartition(this.servers.getSelectedServerId(), parseInt(value, 10), this.topicName);
    }
  }

}
