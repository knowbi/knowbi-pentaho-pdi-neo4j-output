/*
 * Copyright 2017 Hitachi America, Ltd., R&D.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bi.know.kettle.neo4j.perspective;

import org.eclipse.jface.resource.JFaceResources;
import org.eclipse.swt.widgets.Text;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.components.XulTextbox;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;
import org.pentaho.ui.xul.swt.SwtBindingFactory;

import java.lang.reflect.InvocationTargetException;

public class Neo4jController extends AbstractXulEventHandler {

  private static final Class<?> PKG = Neo4jController.class;

  private BindingFactory bf = new SwtBindingFactory();
  private Binding revisionBinding;
  private Binding changedBinding;
  private Binding branchBinding;
  private Binding diffBinding;

  public Neo4jController() {
    setName( "neo4jController" );
  }

  public void init() throws IllegalArgumentException, InvocationTargetException, XulException {
    XulTextbox diffText = (XulTextbox) document.getElementById( "diff" );
    Text text = (Text) diffText.getManagedObject();
    text.setFont( JFaceResources.getFont( JFaceResources.TEXT_FONT ) );
  }

  private void createBindings() {
    // TODO
  }

  public void onTabClose() {
    // Simply close
  }
}