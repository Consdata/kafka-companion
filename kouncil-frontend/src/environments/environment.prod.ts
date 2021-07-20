import {Backend} from '../app/app.backend';

export const environment = {
  production: true,
  backend: Backend.SERVER,
  websocketUrl: `${window.location.host}`
};
