import {Backend} from '../app/app.backend';

export const environment = {
  production: true,
  backend: Backend.DEMO,
  websocketUrl: `${window.location.host}`
};
