/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package bi.know.kettle.neo4j.perspective;

import bi.know.kettle.neo4j.core.Neo4jDefaults;
import bi.know.kettle.neo4j.model.GraphModel;
import bi.know.kettle.neo4j.model.GraphModelUtils;
import bi.know.kettle.neo4j.shared.NeoConnection;
import bi.know.kettle.neo4j.shared.NeoConnectionUtils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.ui.core.dialog.EnterSelectionDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.spoon.ISpoonMenuController;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.dialog.MetaStoreExplorerDialog;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.ui.xul.dom.Document;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;

import java.util.Collections;
import java.util.List;

public class Neo4jHelper extends AbstractXulEventHandler implements ISpoonMenuController {
  protected static Class<?> PKG = Neo4jHelper.class; // for i18n

  private static Neo4jHelper instance = null;

  private Spoon spoon;
  private MetaStoreFactory<NeoConnection> connectionFactory;
  private MetaStoreFactory<GraphModel> modelFactory;
  private VariableSpace space;

  private Neo4jHelper() {
    spoon = Spoon.getInstance();
  }

  public static Neo4jHelper getInstance() {
    if ( instance == null ) {
      instance = new Neo4jHelper(); ;
      instance.spoon.addSpoonMenuController( instance );
      instance.space = new Variables();
      instance.space.initializeVariablesFrom( null );
      instance.connectionFactory = new MetaStoreFactory<NeoConnection>( NeoConnection.class, instance.spoon.getMetaStore(), Neo4jDefaults.NAMESPACE );
      instance.modelFactory = new MetaStoreFactory<GraphModel>( GraphModel.class, instance.spoon.getMetaStore(), Neo4jDefaults.NAMESPACE );
    }
    return instance;
  }

  public String getName() {
    return "neo4jHelper";
  }

  public void updateMenu( Document doc ) {
    // Nothing so far.
  }

  public void createConnection() {
    NeoConnectionUtils.newConnection( spoon.getShell(), space, connectionFactory );
  }

  public void editConnection() {
    try {
      List<String> elementNames = connectionFactory.getElementNames();
      Collections.sort(elementNames);
      String[] names = elementNames.toArray( new String[ 0 ] );

      EnterSelectionDialog dialog = new EnterSelectionDialog( spoon.getShell(), names, "Edit Neo4j connection", "Select the connection to edit" );
      String choice = dialog.open();
      if (choice!=null) {
        NeoConnectionUtils.editConnection( spoon.getShell(), space, connectionFactory, choice );
      }
    } catch(Exception e) {
      new ErrorDialog( spoon.getShell(), "Error", "Error editing Neo4j connection", e );
    }
  }

  public void deleteConnection() {
    try {
      List<String> elementNames = connectionFactory.getElementNames();
      Collections.sort( elementNames );
      String[] names = elementNames.toArray( new String[ 0 ] );

      EnterSelectionDialog dialog = new EnterSelectionDialog( spoon.getShell(), names, "Delete Neo4j connection", "Select the connection to delete" );
      String choice = dialog.open();
      if ( choice != null ) {
        NeoConnectionUtils.deleteConnection( spoon.getShell(), connectionFactory, choice );
      }
    } catch(Exception e) {
      new ErrorDialog( spoon.getShell(), "Error", "Error deleting Neo4j connection", e );
    }
  }

  public void createModel() {
    GraphModelUtils.newModel( spoon.getShell(), modelFactory, null);
  }

  public void editModel() {
    try {
      List<String> elementNames = modelFactory.getElementNames();
      Collections.sort(elementNames);
      String[] names = elementNames.toArray( new String[ 0 ] );

      EnterSelectionDialog dialog = new EnterSelectionDialog( spoon.getShell(), names, "Edit Neo4j model", "Select the graph model to edit" );
      String choice = dialog.open();
      if (choice!=null) {
        GraphModelUtils.editModel( spoon.getShell(), modelFactory, choice, null);
      }
    } catch(Exception e) {
      new ErrorDialog( spoon.getShell(), "Error", "Error editing Neo4j graph model", e );
    }
  }

  public void deleteModel() {
    try {
      List<String> elementNames = modelFactory.getElementNames();
      Collections.sort( elementNames );
      String[] names = elementNames.toArray( new String[ 0 ] );

      EnterSelectionDialog dialog = new EnterSelectionDialog( spoon.getShell(), names, "Delete Neo4j model", "Select the graph model to delete" );
      String choice = dialog.open();
      if ( choice != null ) {
        GraphModelUtils.deleteModel( spoon.getShell(), modelFactory, choice );
      }
    } catch(Exception e) {
      new ErrorDialog( spoon.getShell(), "Error", "Error deleting Neo4j graph model", e );
    }
  }

  public void showMetaStoreBrowser() {
    Spoon spoon = Spoon.getInstance();
    new MetaStoreExplorerDialog( spoon.getShell(), spoon.getMetaStore() ).open();
  }

}
