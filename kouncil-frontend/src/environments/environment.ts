import {Backend} from '../app/app.backend';

export const environment = {
  production: false,
  backend: Backend.SERVER,
  websocketUrl: `${window.location.hostname}:8080`
};
